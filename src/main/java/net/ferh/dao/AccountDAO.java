package net.ferh.dao;

import net.ferh.entity.Account;

import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ferh on 27.07.14.
 */
public class AccountDAO {

    private Connection connection;

    public AccountDAO() {
        init();
    }

    public Collection<Account> getAll() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select id,balance from account");
        List<Account> data = new LinkedList<Account>();
        while (rs.next()) {
            data.add(new Account(rs.getInt("id"), rs.getLong("balance")));
        }
        return data;
    }

    public void save(int id, AtomicLong balance) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("insert into account(id, balance) values (?,?) on DUPLICATE KEY UPDATE balance=?");
        statement.setInt(1, id);
        statement.setLong(2, balance.get());
        statement.setLong(3, balance.get());
        statement.executeUpdate();
    }

    public void init() {
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/account", "root", "root");
            stmt = connection.createStatement();
//            stmt.addBatch("drop table if exists account.account;");
            stmt.addBatch("create table if not exists account(\n" +
                    "  id int not null primary key,\n" +
                    "  balance bigint(20) not null\n" +
                    ");");
            stmt.executeBatch();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
