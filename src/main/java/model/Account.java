package model;

import java.util.concurrent.locks.ReentrantLock;

public class Account {
    private final String accountId;
    private double balance;
    private int shares;
    private final ReentrantLock lock;

    public Account(String accountId, double initialBalance, int initialShares) {
        this(accountId, initialBalance, initialShares, false);
    }

    public Account(String accountId, double initialBalance, int initialShares, boolean fair) {
        this.accountId = accountId;
        this.balance = initialBalance;
        this.shares = initialShares;
        this.lock = new ReentrantLock(fair);
    }

    public String getAccountId() { return accountId; }
    public ReentrantLock getLock() { return lock; }
    public double getBalance() { return balance; }
    public int getShares() { return shares; }

    public void transferTo(Account target, double cashAmount, int sharesAmount) {
        this.balance -= cashAmount;
        this.shares += sharesAmount;
        target.balance += cashAmount;
        target.shares -= sharesAmount;
    }
}