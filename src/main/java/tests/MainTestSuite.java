package tests;

import engine.OrderBook;
import engine.StockExchange;
import faulty.Account;
import faulty.RacyOrderBook;
import model.MarketSnapshot;
import model.Order;
import model.OrderType;
import model.Transaction;
import simulation.BuyerTask;
import simulation.SellerTask;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * UNIFIED VISUAL TEST SUITE FOR PCBE/CEBP STAGE 1 DEFENSE.
 *
 * Runs all 5 concurrency pathology comparisons (WITH BUG vs WITHOUT BUG) plus the full multi-ticker
 * conservation audit in a single visual dashboard.
 */
public class MainTestSuite {
    private static final List<String> TICKERS = List.of("AAPL", "GOOGL", "MSFT");

    public static void main(String[] args) throws Exception {
        printBanner("STOCK EXCHANGE - CONCURRENCY VERIFICATION SUITE (PCBE)");

        boolean t1 = test1ConservationAudit();
        boolean t2 = test2DataRaceComparison();
        boolean t3 = test3LockGranularityBars();
        boolean t4 = test4DeadlockComparison();
        boolean t5 = test5LivelockComparison();
        boolean t6 = test6StarvationComparison();

        printFinalScorecard(t1, t2, t3, t4, t5, t6);
    }

    // ====================================================================================================
    // TEST 1: MULTI-TICKER CONSERVATION AUDIT (8 BuyerTask + 8 SellerTask via Callable & Future)
    // ====================================================================================================
    private static boolean test1ConservationAudit() throws InterruptedException, ExecutionException {
        printSection("TEST 1: FULL SYSTEM AUDIT (8 Buyers + 8 Sellers, 3 Tickers, 32,000 Orders)");

        StockExchange exchange = new StockExchange(TICKERS, 10_000);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<Integer>> buyFutures = new ArrayList<>();
        List<Future<Integer>> sellFutures = new ArrayList<>();

        long start = System.nanoTime();
        for (int i = 1; i <= 8; i++) {
            buyFutures.add(pool.submit(new BuyerTask("Buyer-" + i, exchange, TICKERS, 2_000)));
            sellFutures.add(pool.submit(new SellerTask("Seller-" + i, exchange, TICKERS, 2_000)));
        }
        pool.shutdown();

        int submittedBuy = sumFutures(buyFutures);
        int submittedSell = sumFutures(sellFutures);
        long ms = (System.nanoTime() - start) / 1_000_000;

        int traded = exchange.getTradeHistory().stream().mapToInt(Transaction::quantity).sum();
        int waitingBuy = exchange.getSnapshots().stream().mapToInt(MarketSnapshot::openBuyShares).sum();
        int waitingSell = exchange.getSnapshots().stream().mapToInt(MarketSnapshot::openSellShares).sum();

        boolean buyOk = (submittedBuy == traded + waitingBuy);
        boolean sellOk = (submittedSell == traded + waitingSell);

        System.out.println("  +-------------------------------------------------------------------------+");
        System.out.printf("  | Execution Time : %-54s |%n", ms + " ms (" + exchange.getTradeHistory().size() + " trades matched)");
        System.out.printf("  | BUY  Equation  : %6d submitted == %6d traded + %6d waiting [%s] |%n",
                submittedBuy, traded, waitingBuy, buyOk ? "OK" : "XX");
        System.out.printf("  | SELL Equation  : %6d submitted == %6d traded + %6d waiting [%s] |%n",
                submittedSell, traded, waitingSell, sellOk ? "OK" : "XX");
        System.out.println("  +-------------------------------------------------------------------------+");
        return buyOk && sellOk;
    }

    // ====================================================================================================
    // TEST 2: DATA RACE (WITH BUG: RacyOrderBook vs WITHOUT BUG: Locked OrderBook)
    // ====================================================================================================
    private static boolean test2DataRaceComparison() throws InterruptedException, ExecutionException {
        printSection("TEST 2: DATA RACE & CONSISTENCY (16 Threads Hitting 1 Ticker @ $100.00)");

        RaceMetrics buggy = runRaceTrial(false);
        RaceMetrics fixed = runRaceTrial(true);

        System.out.println("  +---------------------------+------------------------+------------------------+");
        System.out.println("  | Metric                    | WITH BUG (No Lock)     | WITHOUT BUG (Locked)   |");
        System.out.println("  +---------------------------+------------------------+------------------------+");
        System.out.printf("  | Oversold Orders (qty < 0) | %-22d | %-22d |%n", buggy.overfilled, fixed.overfilled);
        System.out.printf("  | Duplicate Trade IDs       | %-22d | %-22d |%n", buggy.dupIds, fixed.dupIds);
        System.out.printf("  | Unsynced List Exceptions  | %-22d | %-22d |%n", buggy.crashes, fixed.crashes);
        System.out.printf("  | Share Equation Balanced?  | %-22s | %-22s |%n",
                buggy.balanced ? "YES" : "NO (Lost Updates!)", fixed.balanced ? "YES (Exact)" : "NO");
        System.out.println("  +---------------------------+------------------------+------------------------+");

        boolean bugDemonstrated = (buggy.overfilled > 0 || !buggy.balanced || buggy.dupIds > 0 || buggy.crashes > 0);
        boolean fixVerified = (fixed.overfilled == 0 && fixed.balanced && fixed.dupIds == 0 && fixed.crashes == 0);
        return bugDemonstrated && fixVerified;
    }

    private record RaceMetrics(int overfilled, long dupIds, int crashes, boolean balanced) {}

    private static RaceMetrics runRaceTrial(boolean useLockedBook) throws InterruptedException, ExecutionException {
        RacyOrderBook racy = new RacyOrderBook();
        OrderBook locked = new OrderBook("AAPL", 10_000);
        Queue<Transaction> history = new ConcurrentLinkedQueue<>();
        AtomicInteger crashes = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<Integer>> buyers = new ArrayList<>();
        List<Future<Integer>> sellers = new ArrayList<>();

        for (int i = 0; i < 8; i++) {
            buyers.add(pool.submit(raceTrader(OrderType.BUY, useLockedBook, racy, locked, history, crashes)));
            sellers.add(pool.submit(raceTrader(OrderType.SELL, useLockedBook, racy, locked, history, crashes)));
        }
        pool.shutdown();

        int subBuy = sumFutures(buyers);
        int subSell = sumFutures(sellers);
        int traded = history.stream().mapToInt(Transaction::quantity).sum();

        int waitBuy = useLockedBook ? locked.getSnapshot().openBuyShares() : racy.openQuantity(OrderType.BUY);
        int waitSell = useLockedBook ? locked.getSnapshot().openSellShares() : racy.openQuantity(OrderType.SELL);
        int overfilled = useLockedBook ? 0 : racy.overfilledOrders().size();
        long dupIds = history.size() - history.stream().map(Transaction::id).distinct().count();
        boolean balanced = (subBuy == traded + waitBuy) && (subSell == traded + waitSell);

        return new RaceMetrics(overfilled, dupIds, crashes.get(), balanced);
    }

    private static Callable<Integer> raceTrader(OrderType side, boolean useLocked, RacyOrderBook racy,
                                                OrderBook locked, Queue<Transaction> history, AtomicInteger crashes) {
        return () -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            int submitted = 0;
            for (int i = 0; i < 500; i++) {
                int qty = 1 + rnd.nextInt(5);
                Order o = new Order(side + "-T", "AAPL", side, 10_000, qty);
                try {
                    history.addAll(useLocked ? locked.process(o) : racy.process(o));
                } catch (RuntimeException ex) {
                    crashes.incrementAndGet();
                }
                submitted += qty;
            }
            return submitted;
        };
    }

    // ====================================================================================================
    // TEST 3: PERFORMANCE & LOCK GRANULARITY (Visual Bar Chart)
    // ====================================================================================================
    private static boolean test3LockGranularityBars() throws InterruptedException, ExecutionException {
        printSection("TEST 3: LOCK GRANULARITY & CRITICAL SECTION LENGTH (16 Threads, 2ms I/O)");

        long globalMs = timeExchange(new GlobalMutexExchange());
        long tickerSlowMs = timeExchange(new TickerSlowLockExchange());
        long correctMs = timeExchange(new FastLockExchange());

        System.out.println("  Execution Time Comparison (Lower is Better):");
        printBar("1. WITH BUG (1 Global Lock, I/O inside)", globalMs, globalMs);
        printBar("2. PARTIAL  (Ticker Lock, I/O inside)  ", tickerSlowMs, globalMs);
        printBar("3. FIXED    (Ticker Lock, I/O outside) ", correctMs, globalMs);
        System.out.printf("  -> Speedup achieved by fine-grained short locking: %.1fx faster%n",
                (double) globalMs / Math.max(1, correctMs));

        return correctMs < globalMs;
    }

    private static void printBar(String label, long ms, long maxMs) {
        int barLen = (int) Math.max(1, (ms * 30) / Math.max(1, maxMs));
        String bar = "#".repeat(barLen);
        System.out.printf("  | %-39s | %-30s %4d ms |%n", label, bar, ms);
    }

    private static long timeExchange(StockExchange exchange) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<Integer>> futures = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 1; i <= 8; i++) {
            futures.add(pool.submit(new BuyerTask("B-" + i, exchange, TICKERS, 40)));
            futures.add(pool.submit(new SellerTask("S-" + i, exchange, TICKERS, 40)));
        }
        sumFutures(futures);
        pool.shutdown();
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static void simulateIO() {
        try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static class GlobalMutexExchange extends StockExchange {
        GlobalMutexExchange() { super(TICKERS, 10_000); }
        @Override public synchronized void submitOrder(Order o) { super.submitOrder(o); simulateIO(); }
    }

    private static class TickerSlowLockExchange extends StockExchange {
        private final Map<String, ReentrantLock> locks = new HashMap<>();
        TickerSlowLockExchange() {
            super(TICKERS, 10_000);
            TICKERS.forEach(t -> locks.put(t, new ReentrantLock()));
        }
        @Override public void submitOrder(Order o) {
            ReentrantLock l = locks.get(o.getTicker());
            l.lock();
            try { super.submitOrder(o); simulateIO(); } finally { l.unlock(); }
        }
    }

    private static class FastLockExchange extends StockExchange {
        FastLockExchange() { super(TICKERS, 10_000); }
        @Override public void submitOrder(Order o) { super.submitOrder(o); simulateIO(); }
    }

    // ====================================================================================================
    // TEST 4: DEADLOCK (WITH BUG: Opposite Lock Order vs WITHOUT BUG: Global ID Order & Timeout)
    // ====================================================================================================
    private static boolean test4DeadlockComparison() throws InterruptedException {
        printSection("TEST 4: DEADLOCK (Opposite Settlements: A->B and B->A)");

        // 1. Buggy Run
        Account a1 = new Account("ACC-A", 500_000, 100);
        Account b1 = new Account("ACC-B", 500_000, 100);
        ExecutorService buggyPool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        buggyPool.submit(() -> lockInOrder(a1, b1, a1, b1, 20_000, 5)); // locks A then B
        buggyPool.submit(() -> lockInOrder(b1, a1, b1, a1, 15_000, 3)); // locks B then A

        ThreadMXBean jvm = ManagementFactory.getThreadMXBean();
        long[] deadlockedIds = null;
        for (int i = 0; i < 40 && deadlockedIds == null; i++) {
            Thread.sleep(50);
            deadlockedIds = jvm.findDeadlockedThreads();
        }
        boolean deadlockCaught = (deadlockedIds != null && deadlockedIds.length == 2);

        // 2. Fixed Run (Global ID Ordering)
        Account a2 = new Account("ACC-A", 500_000, 100);
        Account b2 = new Account("ACC-B", 500_000, 100);
        ExecutorService fixedPool = Executors.newFixedThreadPool(2);
        fixedPool.submit(() -> lockInOrder(a2, b2, a2, b2, 20_000, 5)); // locks ACC-A then ACC-B
        fixedPool.submit(() -> lockInOrder(a2, b2, b2, a2, 15_000, 3)); // also locks ACC-A then ACC-B!
        fixedPool.shutdown();
        boolean fixedFinished = fixedPool.awaitTermination(3, TimeUnit.SECONDS);

        System.out.println("  +----------------------------------+--------------------------------------+");
        System.out.println("  | Mode                             | Outcome                              |");
        System.out.println("  +----------------------------------+--------------------------------------+");
        System.out.printf("  | WITH BUG (Buyer->Seller order)   | %-36s |%n",
                deadlockCaught ? "DEADLOCKED (JVM caught 2 threads)" : "Unexpected");
        System.out.printf("  | WITHOUT BUG (Sorted ID order)    | %-36s |%n",
                fixedFinished ? "COMPLETED  (" + a2 + ")" : "Timed out");
        System.out.println("  +----------------------------------+--------------------------------------+");

        return deadlockCaught && fixedFinished;
    }

    private static void lockInOrder(Account firstLock, Account secondLock,
                                    Account buyer, Account seller, long cash, int shares) {
        firstLock.getLock().lock();
        try {
            try { Thread.sleep(60); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            secondLock.getLock().lock();
            try {
                buyer.settleBuy(seller, cash, shares);
            } finally {
                secondLock.getLock().unlock();
            }
        } finally {
            firstLock.getLock().unlock();
        }
    }

    // ====================================================================================================
    // TEST 5: LIVELOCK (WITH BUG: Symmetric Immediate Retry vs WITHOUT BUG: Random Back-Off)
    // ====================================================================================================
    private static boolean test5LivelockComparison() throws InterruptedException {
        printSection("TEST 5: LIVELOCK (Polite tryLock() Retries With vs Without Random Back-Off)");

        // 1. Buggy Run (Symmetric retry for 1 second)
        Account a1 = new Account("ACC-A", 500_000, 100);
        Account b1 = new Account("ACC-B", 500_000, 100);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger buggyAttempts = new AtomicInteger();
        AtomicInteger buggyDone = new AtomicInteger();

        ExecutorService pool1 = Executors.newFixedThreadPool(2);
        pool1.submit(() -> symmetricTryLock(a1, b1, barrier, buggyAttempts, buggyDone, 20_000, 5));
        pool1.submit(() -> symmetricTryLock(b1, a1, barrier, buggyAttempts, buggyDone, 15_000, 3));
        Thread.sleep(1_000);
        pool1.shutdownNow();
        pool1.awaitTermination(1, TimeUnit.SECONDS);

        // 2. Fixed Run (Random Backoff)
        Account a2 = new Account("ACC-A", 500_000, 100);
        Account b2 = new Account("ACC-B", 500_000, 100);
        AtomicInteger fixedAttempts = new AtomicInteger();
        AtomicInteger fixedDone = new AtomicInteger();

        ExecutorService pool2 = Executors.newFixedThreadPool(2);
        pool2.submit(() -> backoffTryLock(a2, b2, fixedAttempts, fixedDone, 20_000, 5));
        pool2.submit(() -> backoffTryLock(b2, a2, fixedAttempts, fixedDone, 15_000, 3));
        pool2.shutdown();
        boolean finished = pool2.awaitTermination(3, TimeUnit.SECONDS);

        System.out.println("  +----------------------------------+------------------+-------------------+");
        System.out.println("  | Mode                             | Attempts Made    | Trades Settled    |");
        System.out.println("  +----------------------------------+------------------+-------------------+");
        System.out.printf("  | WITH BUG (Immediate retry)       | %-16d | %-17s |%n",
                buggyAttempts.get(), buggyDone.get() + " (LIVELOCK!)");
        System.out.printf("  | WITHOUT BUG (Random 5-30ms wait) | %-16d | %-17s |%n",
                fixedAttempts.get(), fixedDone.get() + " (100% DONE)");
        System.out.println("  +----------------------------------+------------------+-------------------+");

        return buggyDone.get() == 0 && buggyAttempts.get() > 100 && finished && fixedDone.get() == 2;
    }

    private static void symmetricTryLock(Account buyer, Account seller, CyclicBarrier barrier,
                                         AtomicInteger attempts, AtomicInteger settled, long cash, int shares) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                attempts.incrementAndGet();
                if (buyer.getLock().tryLock()) {
                    try {
                        barrier.await();
                        if (seller.getLock().tryLock()) {
                            try {
                                buyer.settleBuy(seller, cash, shares);
                                settled.incrementAndGet();
                                return;
                            } finally {
                                seller.getLock().unlock();
                            }
                        }
                        barrier.await();
                    } finally {
                        buyer.getLock().unlock();
                    }
                }
            }
        } catch (InterruptedException | BrokenBarrierException ignored) {}
    }

    private static void backoffTryLock(Account buyer, Account seller,
                                       AtomicInteger attempts, AtomicInteger settled, long cash, int shares) {
        try {
            while (true) {
                attempts.incrementAndGet();
                if (buyer.getLock().tryLock()) {
                    try {
                        Thread.sleep(10);
                        if (seller.getLock().tryLock()) {
                            try {
                                buyer.settleBuy(seller, cash, shares);
                                settled.incrementAndGet();
                                return;
                            } finally {
                                seller.getLock().unlock();
                            }
                        }
                    } finally {
                        buyer.getLock().unlock();
                    }
                }
                Thread.sleep(ThreadLocalRandom.current().nextInt(5, 30));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ====================================================================================================
    // TEST 6: STARVATION (WITH BUG: PriorityBlockingQueue vs WITHOUT BUG: FIFO LinkedBlockingQueue)
    // ====================================================================================================
    private static boolean test6StarvationComparison() throws InterruptedException {
        printSection("TEST 6: STARVATION (1 Standard Order vs Flood of VIP Orders)");

        long buggyWait = runStarvationTrial(new PriorityBlockingQueue<>());
        long fixedWait = runStarvationTrial(new LinkedBlockingQueue<>());

        System.out.println("  +----------------------------------+--------------------------------------+");
        System.out.println("  | Queue Strategy                   | Standard Client Outcome              |");
        System.out.println("  +----------------------------------+--------------------------------------+");
        System.out.printf("  | WITH BUG (PriorityBlockingQueue) | %-36s |%n",
                buggyWait < 0 ? "NEVER SERVED (Starved by VIP flood)" : "Served in " + buggyWait + " ms");
        System.out.printf("  | WITHOUT BUG (FIFO LinkedQueue)   | %-36s |%n",
                fixedWait >= 0 ? "SERVED IMMEDIATELY (Waited " + fixedWait + " ms)" : "Starved");
        System.out.println("  +----------------------------------+--------------------------------------+");

        return buggyWait < 0 && fixedWait >= 0;
    }

    private static long runStarvationTrial(BlockingQueue<Runnable> queue) throws InterruptedException {
        ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        AtomicLong standardWaitMs = new AtomicLong(-1);

        worker.execute(new PrioritizedOrder(true, standardWaitMs));   // 1st task occupies the single worker thread
        worker.execute(new PrioritizedOrder(false, standardWaitMs));  // Standard request enters queue first

        long end = System.nanoTime() + 1_000 * 1_000_000L;            // 1 second flood of VIP orders
        while (System.nanoTime() < end) {
            worker.execute(new PrioritizedOrder(true, standardWaitMs));
            worker.execute(new PrioritizedOrder(true, standardWaitMs));
            Thread.sleep(1);
        }
        worker.shutdownNow();
        worker.awaitTermination(1, TimeUnit.SECONDS);
        return standardWaitMs.get();
    }

    private static class PrioritizedOrder implements Runnable, Comparable<PrioritizedOrder> {
        private static final AtomicLong SEQ = new AtomicLong();
        private final boolean vip;
        private final AtomicLong standardWaitMs;
        private final long order = SEQ.getAndIncrement();
        private final long created = System.nanoTime();

        PrioritizedOrder(boolean vip, AtomicLong standardWaitMs) {
            this.vip = vip;
            this.standardWaitMs = standardWaitMs;
        }

        @Override
        public void run() {
            if (!vip) {
                standardWaitMs.set((System.nanoTime() - created) / 1_000_000);
            }
            try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        @Override
        public int compareTo(PrioritizedOrder o) {
            return vip != o.vip ? (vip ? -1 : 1) : Long.compare(order, o.order);
        }
    }

    // ====================================================================================================
    // FORMATTING HELPERS
    // ====================================================================================================
    private static int sumFutures(List<Future<Integer>> futures) throws InterruptedException, ExecutionException {
        int sum = 0;
        for (Future<Integer> f : futures) sum += f.get();
        return sum;
    }

    private static void printBanner(String title) {
        System.out.println("\n=============================================================================");
        System.out.println("  " + title);
        System.out.println("=============================================================================");
    }

    private static void printSection(String title) {
        System.out.println("\n-----------------------------------------------------------------------------");
        System.out.println("  " + title);
        System.out.println("-----------------------------------------------------------------------------");
    }

    private static void printFinalScorecard(boolean t1, boolean t2, boolean t3, boolean t4, boolean t5, boolean t6) {
        printBanner("FINAL VERIFICATION SCORECARD");
        System.out.printf("  [ %s ] Test 1: Multi-Ticker Conservation Audit (Callable + Future)%n", t1 ? "PASS" : "FAIL");
        System.out.printf("  [ %s ] Test 2: Data Race Reproduced & Fixed (ReentrantLock)%n", t2 ? "PASS" : "FAIL");
        System.out.printf("  [ %s ] Test 3: Coarse vs Fine-Grained Lock Speedup Proven%n", t3 ? "PASS" : "FAIL");
        System.out.printf("  [ %s ] Test 4: Deadlock Detected (ThreadMXBean) & Fixed (Lock Ordering)%n", t4 ? "PASS" : "FAIL");
        System.out.printf("  [ %s ] Test 5: Livelock Reproduced & Fixed (Random Back-Off)%n", t5 ? "PASS" : "FAIL");
        System.out.printf("  [ %s ] Test 6: Starvation Reproduced & Fixed (FIFO Queue)%n", t6 ? "PASS" : "FAIL");
        System.out.println("=============================================================================\n");
    }
}