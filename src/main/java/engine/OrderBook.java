package engine;

import model.MarketSnapshot;
import model.Order;
import model.OrderType;
import model.Transaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The market of ONE ticker: the waiting buy orders (bids), the waiting sell orders (asks) and the matching logic.
 *
 * HOW THIS CLASS IS MADE THREAD-SAFE
 *  1. One ReentrantLock per OrderBook = FINE-GRAINED locking. Threads working on AAPL and on GOOGL use different
 *     locks, so they run in parallel. Only threads touching the SAME ticker wait for each other.
 *  2. The lock guards bids, asks, nextTradeId and the remainingQuantity of every order inside the book.
 *     It also gives the rule "no modification during a transaction": cancel() and process() take the same lock,
 *     so a cancel can never overlap a match.
 *  3. The critical section is as SHORT as possible: queue operations and quantity updates only. No I/O, no random
 *     numbers, no sleeping, no callbacks. Trades are RETURNED; the caller records them after the lock is released.
 *  4. A thread never holds two locks at once, so this class cannot be part of a deadlock.
 *  5. lastPriceCents is volatile: written under the lock (one writer at a time), read by many threads WITHOUT the lock.
 *
 * Why ReentrantLock and not synchronized? Same mutual exclusion, but lock()/try/finally/unlock() shows exactly where
 * the critical section starts and ends, and the class also offers tryLock, timeouts and fairness (see faulty/).
 */
public class OrderBook {
    private final String ticker;
    private final ReentrantLock lock = new ReentrantLock();

    // Best bid = highest price first. Best ask = lowest price first. Equal price: older order (smaller id) first.
    private final PriorityQueue<Order> bids = new PriorityQueue<>(
            Comparator.comparingLong(Order::getPriceCents).reversed().thenComparingLong(Order::getId));
    private final PriorityQueue<Order> asks = new PriorityQueue<>(
            Comparator.comparingLong(Order::getPriceCents).thenComparingLong(Order::getId));

    private long nextTradeId = 1;           // plain long: only touched inside the lock, so no AtomicLong needed
    private volatile long lastPriceCents;   // price of the latest trade (the opening price before the first one)

    public OrderBook(String ticker, long openingPriceCents) {
        this.ticker = ticker;
        this.lastPriceCents = openingPriceCents;
    }

    /** Matches a new order against the book. Returns the trades it caused (empty if it just starts waiting). */
    public List<Transaction> process(Order incoming) {
        List<Transaction> trades = new ArrayList<>();   // allocated BEFORE taking the lock
        lock.lock();                                    // ---- critical section starts
        try {
            match(incoming, trades);
        } finally {
            lock.unlock();                              // ---- always released, even if match() throws
        }
        return trades;                                  // the caller publishes them OUTSIDE the lock
    }

    /**
     * Withdraws a waiting order. Returns how many shares were still open, or 0 if the order is no longer in the book
     * (it was completely traded in the meantime).
     */
    public int cancel(Order order) {
        lock.lock();
        try {
            PriorityQueue<Order> ownSide = (order.getType() == OrderType.BUY) ? bids : asks;
            return ownSide.remove(order) ? order.getRemainingQuantity() : 0;
        } finally {
            lock.unlock();
        }
    }

    /** Cheap read of the latest trade price: no lock, just a volatile read. */
    public long getLastPriceCents() {
        return lastPriceCents;
    }

    /** A consistent picture of this market. Scans both queues, so it is meant for reporting, not for the hot path. */
    public MarketSnapshot getSnapshot() {
        lock.lock();
        try {
            return new MarketSnapshot(ticker, lastPriceCents,
                    bids.isEmpty() ? MarketSnapshot.NONE : bids.peek().getPriceCents(),
                    asks.isEmpty() ? MarketSnapshot.NONE : asks.peek().getPriceCents(),
                    totalRemaining(bids), totalRemaining(asks));
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Everything below runs while the caller holds the lock.
    // ------------------------------------------------------------------------------------------------------------

    private void match(Order incoming, List<Transaction> trades) {
        boolean isBuy = incoming.getType() == OrderType.BUY;
        PriorityQueue<Order> oppositeSide = isBuy ? asks : bids;
        PriorityQueue<Order> ownSide = isBuy ? bids : asks;

        // Trade against the best waiting orders for as long as the prices fit and the incoming order is not filled.
        while (!incoming.isFilled() && !oppositeSide.isEmpty() && incoming.canTradeWith(oppositeSide.peek())) {
            Order resting = oppositeSide.peek();
            int quantity = Math.min(incoming.getRemainingQuantity(), resting.getRemainingQuantity());  // n = min(offer, request)

            incoming.fill(quantity);
            resting.fill(quantity);
            trades.add(Transaction.between(nextTradeId++, incoming, resting, quantity));
            lastPriceCents = resting.getPriceCents();

            if (resting.isFilled()) {
                oppositeSide.poll();
            }
        }

        // Whatever is left waits in the book for a future counterpart.
        if (!incoming.isFilled()) {
            ownSide.add(incoming);
        }
    }

    private static int totalRemaining(PriorityQueue<Order> orders) {
        int total = 0;
        for (Order order : orders) {
            total += order.getRemainingQuantity();
        }
        return total;
    }
}
