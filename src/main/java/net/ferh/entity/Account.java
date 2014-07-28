package net.ferh.entity;

/**
 * Created by ferh on 28.07.14.
 */
public class Account {
    private final int id;
    private final long balance;

    public Account(int id, long balance) {
        this.id = id;
        this.balance = balance;
    }

    public int getId() {
        return id;
    }

    public long getBalance() {
        return balance;
    }
}