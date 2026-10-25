package faulty;

import engine.StockExchange;
import model.Order;
import simulation.BuyerTask;
import simulation.SellerTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FAULTY VERSION #2c: PERFORMANCE. Nothing is "wrong" in the sense of wrong results, but the locking is too coarse and
 * the program becomes SEQUENTIAL: 16 threads exist, but only one of them works at a time.
 *
 * The same 16 traders (the real BuyerTask / SellerTask) run against three exchanges that differ ONLY in their locking.
 * "Slow work" stands for I/O such as writing an audit log or sending a confirmation (simulated with a 2 ms sleep).
 *
 *   1) one global lock, slow work inside it  -> every order waits for every other order       (worst)
 *   2) one lock per ticker, slow work inside -> AAPL, GOOGL, MSFT run in parallel: at most 3x (granularity fixed)
 *   3) one lock per ticker, slow work outside-> locks are held for microseconds only            (what we ship)
 *
 * Lesson: reduce HOW MANY threads share a lock (granularity) and HOW LONG it is held (critical-section length).
 */
public class MainCoarseLock {
    private static final List<String> TICKERS = List.of("AAPL", "GOOGL", "MSFT");
    private static final long OPENING_PRICE_CENTS = 10_000;
    private static final int BUYERS = 8;
    private static final int SELLERS = 8;
    private static final int ORDERS_PER_TRADER = 50;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.out.println("16 traders, 50 orders each, 2 ms of slow work per order. Only the locking differs:\n");
        time("1) FAULTY  global lock, slow work inside lock ", new GlobalLockExchange());
        time("2) FAULTY  ticker locks, slow work inside lock", new TickerLockExchange());
        time("3) CORRECT ticker locks, slow work outside    ", new CorrectExchange());
    }

    private static void time(String label, StockExchange exchange) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(BUYERS + SELLERS);
        List<Future<Integer>> results = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 1; i <= BUYERS; i++) {
            results.add(pool.submit(new BuyerTask("Buyer-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        for (int i = 1; i <= SELLERS; i++) {
            results.add(pool.submit(new SellerTask("Seller-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        for (Future<Integer> result : results) {
            result.get();
        }
        pool.shutdown();
        System.out.printf("%s: %5d ms%n", label, (System.nanoTime() - start) / 1_000_000);
    }

    /** Stand-in for I/O (audit log, network confirmation). */
    private static void slowWork() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** FAULTY: ONE lock for the whole exchange, held during the slow work. All 16 threads queue up behind it. */
    static class GlobalLockExchange extends StockExchange {
        private final Object globalLock = new Object();

        GlobalLockExchange() {
            super(TICKERS, OPENING_PRICE_CENTS);
        }

        @Override
        public void submitOrder(Order order) {
            synchronized (globalLock) {
                super.submitOrder(order);
                slowWork();
            }
        }
    }

    /** FAULTY: a lock per ticker (good granularity), but the slow work still happens while holding it. */
    static class TickerLockExchange extends StockExchange {
        private final Map<String, ReentrantLock> locks = new HashMap<>();

        TickerLockExchange() {
            super(TICKERS, OPENING_PRICE_CENTS);
            TICKERS.forEach(ticker -> locks.put(ticker, new ReentrantLock()));
        }

        @Override
        public void submitOrder(Order order) {
            ReentrantLock lock = locks.get(order.getTicker());
            lock.lock();
            try {
                super.submitOrder(order);
                slowWork();
            } finally {
                lock.unlock();
            }
        }
    }

    /** CORRECT: the only lock is the short one inside OrderBook; the slow work runs outside every lock. */
    static class CorrectExchange extends StockExchange {
        CorrectExchange() {
            super(TICKERS, OPENING_PRICE_CENTS);
        }

        @Override
        public void submitOrder(Order order) {
            super.submitOrder(order);
            slowWork();
        }
    }
}
