package net.ferh.server;

import net.ferh.common.AccountService;
import net.ferh.dao.AccountDAO;
import net.ferh.entity.Account;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ferh on 27.07.14.
 */
public class AccountServiceImpl extends AbstractHandler implements AccountService {

    private Map<Integer, AtomicLong> cache = new ConcurrentHashMap<Integer, AtomicLong>();
    private ExecutorService executorService = Executors.newFixedThreadPool(256);
    private AccountDAO accountDAO = new AccountDAO();
    private static ServerStats serverStats = new ServerStats();

    public AccountServiceImpl() {
        super();
        readDataFromDB();
    }

    private void readDataFromDB() {
        try {
            Collection<Account> all = accountDAO.getAll();
            for (Account acc : all) {
                cache.put(acc.getId(), new AtomicLong(acc.getBalance()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Long getAmount(Integer id) throws RemoteException {
        AtomicLong balance = cache.get(id);
        serverStats.incrementReadQueries();
        if (balance == null) {
            return 0L;
        } else {
            return balance.get();
        }
    }

    @Override
    public void addAmount(final Integer id, Long value) throws RemoteException {
        AtomicLong balance = cache.get(id);
        serverStats.incrementWriteQueries();
        if (balance == null) {
            balance = new AtomicLong(value);
            cache.put(id, balance);
        } else {
            balance.addAndGet(value);
            cache.put(id, balance);
            final AtomicLong finalBalance = balance;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        accountDAO.save(id, finalBalance);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {
        if (s.equals("/stats")) {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            request.setHandled(true);
            response.getWriter().println("Current time    :" + System.currentTimeMillis() + "<br>");
            response.getWriter().println("Server running  :" + serverStats.getStartTime() + "<br>");
            response.getWriter().println("READ   :" + serverStats.getTotalReadQueries() + "ReadRPS :" + serverStats.getReadRPS() + "<br>");
            response.getWriter().println("WRITE  :" + serverStats.getTotalWriteQueries() + "WriteRPS :" + serverStats.getWriteRPS() + "<br>");
            response.getWriter().println("TOTAL RPS  :" + serverStats.getTotalRPS() + "<br>");
            serverStats.logStats();
        } else if (s.equals("/flush")) {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            request.setHandled(true);
            serverStats.setTotalReadQueries(new AtomicLong(0));
            serverStats.setTotalWriteQueries(new AtomicLong(0));
            serverStats.setStartTime(new AtomicLong(System.currentTimeMillis()));
            response.getWriter().println("DB stats flushed");
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            String name = "AccountService";
            AccountService accountService = new AccountServiceImpl();
            Remote stub = UnicastRemoteObject.exportObject(accountService, 0);
            Registry registry = LocateRegistry.createRegistry(8080);
            registry.bind(name, stub);
            System.out.println("AccountService bound");
        } catch (Exception e) {
            System.err.println("AccountService exception");
            e.printStackTrace();
        }
        Server server = new Server(8088);
        server.setHandler(new AccountServiceImpl());
        server.start();
        server.join();
    }
}
