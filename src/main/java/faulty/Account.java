package faulty;

import model.Money;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A trading account (cash + shares) with its own lock. Settling a trade changes TWO accounts (buyer and seller),
 * so it needs two locks at the same time: exactly the situation in which deadlock and livelock appear.
 * Used by MainDeadlock and MainLivelock.
 */
public class Account {
    private final String id;
    private long cashCents;
    private int shares;
    private final ReentrantLock lock = new ReentrantLock();

    public Account(String id, long cashCents, int shares) {
        this.id = id;
        this.cashCents = cashCents;
        this.shares = shares;
    }

    public String getId() { return id; }
    public ReentrantLock getLock() { return lock; }
    public long getCashCents() { return cashCents; }
    public int getShares() { return shares; }

    /** THIS account buys: it pays {@code cash} to the seller and receives {@code shares}. The caller must hold BOTH locks. */
    public void settleBuy(Account seller, long cash, int shares) {
        this.cashCents -= cash;
        this.shares += shares;
        seller.cashCents += cash;
        seller.shares -= shares;
    }

    @Override
    public String toString() {
        return id + ": cash " + Money.format(cashCents) + ", " + shares + " shares";
    }
}
