package tests;

import faulty.Account;
import model.Order;
import model.OrderType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * STEP-BY-STEP CHRONOLOGICAL TRACE VISUALIZER.
 *
 * Shows the exact timeline (Step 1 -> Step 2 -> Step 3...) of two threads interacting,
 * highlighting the exact step where each concurrency pathology occurs vs. how the fix prevents it.
 */
public class MainStepVisualizer {

    public static void main(String[] args) throws Exception {
        System.out.println("====================================================================================");
        System.out.println("  STEP-BY-STEP CONCURRENCY TRACE: WHERE & HOW EACH BUG HAPPENS");
        System.out.println("====================================================================================");

        trace1DataRace();
        trace2CoarseVsFineLock();
        trace3Deadlock();
        trace4Livelock();
        trace5Starvation();
    }

    // ====================================================================================================
    // CASE 1: DATA RACE (Check-Then-Act & Lost Update on a Single Order)
    // ====================================================================================================
    private static void trace1DataRace() throws InterruptedException {
        header("CASE 1: DATA RACE (Two Buyers trying to take the LAST 5 shares of a Sell Order)");

        // --- A) WITH BUG (No Lock) ---
        System.out.println("  [A] WITH BUG (Unsynchronized Check-Then-Act):");
        Order restingSell = new Order("Seller-1", "AAPL", OrderType.SELL, 10_000, 5);
        CyclicBarrier sync = new CyclicBarrier(2);

        Runnable racyBuyer = () -> {
            String me = Thread.currentThread().getName();
            try {
                sync.await();
                // STEP 1: The CHECK
                boolean available = !restingSell.isFilled();
                int seenQty = restingSell.getRemainingQuantity();
                log(me, "STEP 1 (CHECK)  : Sees !isFilled()=" + available + ", remainingQty=" + seenQty + " shares");

                sync.await(); // Both threads pass the check before either subtracts!
                if (me.equals("Buyer-1")) {
                    System.out.println("      >>> [BUG OCCURS AT STEP 2]: Both threads passed the IF check before either updated quantity!");
                }
                sync.await();

                // STEP 2: The ACT
                restingSell.fill(5);
                log(me, "STEP 2 (ACT)    : Calls fill(5) -> remainingQty is now " + restingSell.getRemainingQuantity());
            } catch (Exception ignored) {}
        };

        runTwoThreads("Buyer-1", "Buyer-2", racyBuyer, racyBuyer);
        System.out.println("      RESULT (WITH BUG)   : Order remainingQuantity = " + restingSell.getRemainingQuantity()
                + " (OVERSOLD! 10 shares sold from a 5-share order)\n");

        // --- B) WITHOUT BUG (With ReentrantLock) ---
        System.out.println("  [B] WITHOUT BUG (Protected by ReentrantLock):");
        Order lockedSell = new Order("Seller-1", "AAPL", OrderType.SELL, 10_000, 5);
        ReentrantLock lock = new ReentrantLock();
        CountDownLatch firstEntered = new CountDownLatch(1);

        Runnable safeBuyer = () -> {
            String me = Thread.currentThread().getName();
            if (me.equals("Buyer-2")) {
                await(firstEntered);
                log(me, "STEP 1 (WAIT)   : Calls lock.lock() -> BLOCKED outside critical section");
            }
            lock.lock();
            try {
                if (me.equals("Buyer-1")) {
                    log(me, "STEP 1 (LOCK)   : Acquires lock. Sees remainingQty=" + lockedSell.getRemainingQuantity());
                    firstEntered.countDown();
                    sleep(50);
                }
                if (!lockedSell.isFilled()) {
                    lockedSell.fill(5);
                    log(me, "STEP 2 (MATCH)  : Buys 5 shares -> remainingQty is now " + lockedSell.getRemainingQuantity());
                } else {
                    log(me, "STEP 3 (REJECT) : Acquires lock, sees remainingQty=" + lockedSell.getRemainingQuantity()
                            + " (isFilled=true). Cannot buy!");
                }
            } finally {
                log(me, "STEP 4 (UNLOCK) : Releases lock");
                lock.unlock();
            }
        };

        runTwoThreads("Buyer-1", "Buyer-2", safeBuyer, safeBuyer);
        System.out.println("      RESULT (WITHOUT BUG): Order remainingQuantity = " + lockedSell.getRemainingQuantity()
                + " (Exact: Buyer-1 got 5, Buyer-2 saw 0)");
    }

    // ====================================================================================================
    // CASE 2: COARSE LOCK VS FINE-GRAINED LOCK (AAPL vs GOOGL)
    // ====================================================================================================
    private static void trace2CoarseVsFineLock() throws InterruptedException {
        header("CASE 2: LOCK GRANULARITY (Thread-1 trades AAPL while Thread-2 trades GOOGL)");

        // --- A) WITH BUG (1 Global Lock) ---
        System.out.println("  [A] WITH BUG (Single Global Mutex on StockExchange):");
        ReentrantLock globalLock = new ReentrantLock();
        CountDownLatch t1Inside = new CountDownLatch(1);
        long startBug = System.nanoTime();

        Runnable aaplGlobal = () -> {
            globalLock.lock();
            try {
                logTime(startBug, "Trader-AAPL ", "STEP 1: Grabs GLOBAL lock to trade AAPL (holds for 100ms)");
                t1Inside.countDown();
                sleep(100);
                logTime(startBug, "Trader-AAPL ", "STEP 3: Releases GLOBAL lock");
            } finally {
                globalLock.unlock();
            }
        };
        Runnable googlGlobal = () -> {
            await(t1Inside);
            logTime(startBug, "Trader-GOOGL", "STEP 2: Wants to trade GOOGL -> BLOCKED by AAPL! (Unnecessary wait)");
            System.out.println("      >>> [ISSUE OCCURS AT STEP 2]: GOOGL thread is frozen waiting for unrelated AAPL lock!");
            globalLock.lock();
            try {
                logTime(startBug, "Trader-GOOGL", "STEP 4: Finally gets GLOBAL lock after AAPL finishes (100ms wasted)");
                sleep(100);
            } finally {
                globalLock.unlock();
            }
        };
        runTwoThreads("Trader-AAPL", "Trader-GOOGL", aaplGlobal, googlGlobal);
        System.out.printf("      RESULT (WITH BUG)   : Sequential execution took ~%d ms%n%n",
                (System.nanoTime() - startBug) / 1_000_000);

        // --- B) WITHOUT BUG (Per-Ticker Lock) ---
        System.out.println("  [B] WITHOUT BUG (Separate ReentrantLock per OrderBook):");
        ReentrantLock aaplLock = new ReentrantLock();
        ReentrantLock googlLock = new ReentrantLock();
        CyclicBarrier bothStart = new CyclicBarrier(2);
        long startFix = System.nanoTime();

        Runnable aaplFine = () -> {
            awaitBarrier(bothStart);
            aaplLock.lock();
            try {
                logTime(startFix, "Trader-AAPL ", "STEP 1: Grabs AAPL lock  -> trading AAPL in parallel (100ms)");
                sleep(100);
                logTime(startFix, "Trader-AAPL ", "STEP 2: Releases AAPL lock");
            } finally {
                aaplLock.unlock();
            }
        };
        Runnable googlFine = () -> {
            awaitBarrier(bothStart);
            googlLock.lock();
            try {
                logTime(startFix, "Trader-GOOGL", "STEP 1: Grabs GOOGL lock -> trading GOOGL at the EXACT same time!");
                sleep(100);
                logTime(startFix, "Trader-GOOGL", "STEP 2: Releases GOOGL lock");
            } finally {
                googlLock.unlock();
            }
        };
        runTwoThreads("Trader-AAPL", "Trader-GOOGL", aaplFine, googlFine);
        System.out.printf("      RESULT (WITHOUT BUG): True parallel execution took ~%d ms (2x faster for 2 tickers)%n",
                (System.nanoTime() - startFix) / 1_000_000);
    }

    // ====================================================================================================
    // CASE 3: DEADLOCK (Circular Wait vs Global Lock Ordering)
    // ====================================================================================================
    private static void trace3Deadlock() throws InterruptedException {
        header("CASE 3: DEADLOCK (Opposite Trades: Thread-1 [A->B] vs Thread-2 [B->A])");

        // --- A) WITH BUG (Opposite Lock Order) ---
        System.out.println("  [A] WITH BUG (Lock Buyer first, then Seller):");
        Account accA = new Account("ACC-A", 500_000, 100);
        Account accB = new Account("ACC-B", 500_000, 100);
        CyclicBarrier step1Done = new CyclicBarrier(2);

        ExecutorService pool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        pool.submit(() -> {
            accA.getLock().lock();
            log("Thread-1 (A->B)", "STEP 1: Locked ACC-A (Buyer)");
            awaitBarrier(step1Done);
            log("Thread-1 (A->B)", "STEP 2: Requesting lock on ACC-B (Seller)... WAITING FOREVER");
            accB.getLock().lock();
        });

        pool.submit(() -> {
            accB.getLock().lock();
            log("Thread-2 (B->A)", "STEP 1: Locked ACC-B (Buyer)");
            awaitBarrier(step1Done);
            log("Thread-2 (B->A)", "STEP 2: Requesting lock on ACC-A (Seller)... WAITING FOREVER");
            accA.getLock().lock();
        });

        sleep(150);
        ThreadMXBean jvm = ManagementFactory.getThreadMXBean();
        long[] deadlocked = jvm.findDeadlockedThreads();
        System.out.println("      >>> [DEADLOCK OCCURS AT STEP 2]: Thread-1 holds ACC-A & waits for ACC-B; Thread-2 holds ACC-B & waits for ACC-A!");
        System.out.println("      RESULT (WITH BUG)   : JVM ThreadMXBean detected "
                + (deadlocked == null ? 0 : deadlocked.length) + " deadlocked threads (Circular Wait).\n");

        // --- B) WITHOUT BUG (Global ID Lock Ordering) ---
        System.out.println("  [B] WITHOUT BUG (Global Lock Order: Always lock smaller ID 'ACC-A' first):");
        Account fixA = new Account("ACC-A", 500_000, 100);
        Account fixB = new Account("ACC-B", 500_000, 100);
        CountDownLatch t1HasA = new CountDownLatch(1);

        Runnable t1Ordered = () -> {
            fixA.getLock().lock();
            try {
                log("Thread-1 (A->B)", "STEP 1: Locked ACC-A first (min ID)");
                t1HasA.countDown();
                sleep(50);
                fixB.getLock().lock();
                try {
                    log("Thread-1 (A->B)", "STEP 2: Locked ACC-B second -> settles trade A->B");
                    fixA.settleBuy(fixB, 20_000, 5);
                } finally {
                    fixB.getLock().unlock();
                }
            } finally {
                log("Thread-1 (A->B)", "STEP 3: Unlocked ACC-B and ACC-A");
                fixA.getLock().unlock();
            }
        };

        Runnable t2Ordered = () -> {
            await(t1HasA);
            log("Thread-2 (B->A)", "STEP 1: Also tries to lock ACC-A first (min ID) -> waits cleanly behind Thread-1");
            fixA.getLock().lock();
            try {
                fixB.getLock().lock();
                try {
                    log("Thread-2 (B->A)", "STEP 4: Acquired ACC-A then ACC-B -> settles trade B->A");
                    fixB.settleBuy(fixA, 15_000, 3);
                } finally {
                    fixB.getLock().unlock();
                }
            } finally {
                fixA.getLock().unlock();
            }
        };

        runTwoThreads("Thread-1", "Thread-2", t1Ordered, t2Ordered);
        System.out.println("      RESULT (WITHOUT BUG): Both finished cleanly -> " + fixA + " | " + fixB);
    }

    // ====================================================================================================
    // CASE 4: LIVELOCK (Polite Symmetric Retries vs Randomized Back-Off)
    // ====================================================================================================
    private static void trace4Livelock() throws InterruptedException {
        header("CASE 4: LIVELOCK (Polite tryLock() in Lockstep vs Random Back-Off)");

        // --- A) WITH BUG (Symmetric Immediate Retry - showing first 3 cycles) ---
        System.out.println("  [A] WITH BUG (Both threads retry immediately at the exact same time):");
        Account a = new Account("ACC-A", 500_000, 100);
        Account b = new Account("ACC-B", 500_000, 100);
        CyclicBarrier barrier = new CyclicBarrier(2);

        Runnable politeA = () -> politeCycle("Thread-1 (A->B)", a, b, barrier, true);
        Runnable politeB = () -> politeCycle("Thread-2 (B->A)", b, a, barrier, false);
        runTwoThreads("Thread-1", "Thread-2", politeA, politeB);
        System.out.println("      RESULT (WITH BUG)   : 0 settlements completed. Threads repeat this cycle forever!\n");

        // --- B) WITHOUT BUG (Random Back-Off breaks symmetry) ---
        System.out.println("  [B] WITHOUT BUG (Random sleep after failed tryLock breaks symmetry):");
        Account fixA = new Account("ACC-A", 500_000, 100);
        Account fixB = new Account("ACC-B", 500_000, 100);
        CyclicBarrier firstCollision = new CyclicBarrier(2);

        Runnable backoff1 = () -> backoffTrace("Thread-1 (A->B)", fixA, fixB, firstCollision, 5, 20_000, 5);
        Runnable backoff2 = () -> backoffTrace("Thread-2 (B->A)", fixB, fixA, firstCollision, 35, 15_000, 3);
        runTwoThreads("Thread-1", "Thread-2", backoff1, backoff2);
        System.out.println("      RESULT (WITHOUT BUG): Symmetry broken on retry -> " + fixA + " | " + fixB);
    }

    private static void politeCycle(String name, Account first, Account second, CyclicBarrier barrier, boolean leader) {
        for (int cycle = 1; cycle <= 2; cycle++) {
            first.getLock().tryLock();
            awaitBarrier(barrier);
            boolean gotSecond = second.getLock().tryLock(); // Always false!
            if (leader) {
                log("Both Threads", "CYCLE " + cycle + " - STEP 1: T1 holds ACC-A, T2 holds ACC-B");
                log("Both Threads", "CYCLE " + cycle + " - STEP 2: Both call tryLock() on second account -> returns " + gotSecond);
                System.out.println("      >>> [LIVELOCK OCCURS HERE]: Both politely unlock their 1st lock & retry at the exact same instant!");
            }
            awaitBarrier(barrier);
            first.getLock().unlock();
            awaitBarrier(barrier);
        }
    }

    private static void backoffTrace(String name, Account buyer, Account seller, CyclicBarrier collision,
                                     int sleepMs, long cash, int shares) {
        // Attempt 1: collide on purpose
        buyer.getLock().tryLock();
        awaitBarrier(collision);
        if (!seller.getLock().tryLock()) {
            log(name, "ATTEMPT 1: Got " + buyer.getId() + ", failed " + seller.getId()
                    + " -> unlocks & sleeps RANDOM " + sleepMs + " ms");
            buyer.getLock().unlock();
        }
        sleep(sleepMs); // Different random sleeps!

        // Attempt 2: staggered wakeup
        while (true) {
            if (buyer.getLock().tryLock()) {
                try {
                    if (seller.getLock().tryLock()) {
                        try {
                            buyer.settleBuy(seller, cash, shares);
                            log(name, "ATTEMPT 2: Woke up after " + sleepMs + " ms, acquired BOTH locks & settled!");
                            return;
                        } finally {
                            seller.getLock().unlock();
                        }
                    }
                } finally {
                    buyer.getLock().unlock();
                }
            }
            sleep(ThreadLocalRandom.current().nextInt(5, 20));
        }
    }

    // ====================================================================================================
    // CASE 5: STARVATION (Priority Queue Jumping vs FIFO Queue)
    // ====================================================================================================
    private static void trace5Starvation() throws InterruptedException {
        header("CASE 5: STARVATION (1 Standard Order vs Continuous VIP Orders in Queue)");

        // --- A) WITH BUG (PriorityBlockingQueue) ---
        System.out.println("  [A] WITH BUG (PriorityBlockingQueue - VIPs always cut in front of Standard):");
        PriorityBlockingQueue<TraceTask> pq = new PriorityBlockingQueue<>();
        pq.add(new TraceTask("Standard-Order#1", false, 1));
        log("Queue", "STEP 1: Standard-Order#1 arrives FIRST -> Queue: [Standard-Order#1]");

        pq.add(new TraceTask("VIP-Order#2", true, 2));
        pq.add(new TraceTask("VIP-Order#3", true, 3));
        log("Queue", "STEP 2: VIP-Order#2 and VIP-Order#3 arrive LATER");
        System.out.println("      >>> [STARVATION OCCURS AT STEP 2]: VIPs jump to head of queue -> Queue is now: [VIP#2, VIP#3, Standard#1]");

        log("Worker", "STEP 3: Worker pops head -> serves " + pq.poll().name);
        pq.add(new TraceTask("VIP-Order#4", true, 4));
        log("Queue", "STEP 4: VIP-Order#4 arrives -> jumps ahead of Standard#1 again!");
        log("Worker", "STEP 5: Worker pops head -> serves " + pq.poll().name);
        log("Worker", "STEP 6: Worker pops head -> serves " + pq.poll().name);
        System.out.println("      RESULT (WITH BUG)   : Remaining unserved in queue = " + pq.peek().name + " (STARVED!)\n");

        // --- B) WITHOUT BUG (LinkedBlockingQueue - FIFO) ---
        System.out.println("  [B] WITHOUT BUG (LinkedBlockingQueue - Strict First-Come, First-Served):");
        LinkedBlockingQueue<TraceTask> fifo = new LinkedBlockingQueue<>();
        fifo.add(new TraceTask("Standard-Order#1", false, 1));
        fifo.add(new TraceTask("VIP-Order#2", true, 2));
        fifo.add(new TraceTask("VIP-Order#3", true, 3));
        log("Queue", "STEP 1: Arrivals in order -> Queue: [Standard-Order#1, VIP-Order#2, VIP-Order#3]");
        log("Worker", "STEP 2: Worker pops head -> serves " + fifo.poll().name + " FIRST (0 starvation!)");
        log("Worker", "STEP 3: Worker pops head -> serves " + fifo.poll().name);
        log("Worker", "STEP 4: Worker pops head -> serves " + fifo.poll().name);
        System.out.println("      RESULT (WITHOUT BUG): Standard-Order#1 was served immediately in arrival order.");
    }

    private static class TraceTask implements Comparable<TraceTask> {
        final String name;
        final boolean vip;
        final int seq;

        TraceTask(String name, boolean vip, int seq) {
            this.name = name;
            this.vip = vip;
            this.seq = seq;
        }

        @Override
        public int compareTo(TraceTask other) {
            return vip != other.vip ? (vip ? -1 : 1) : Integer.compare(seq, other.seq);
        }
    }

    // ====================================================================================================
    // HELPERS
    // ====================================================================================================
    private static void runTwoThreads(String name1, String name2, Runnable r1, Runnable r2) throws InterruptedException {
        Thread t1 = new Thread(r1, name1);
        Thread t2 = new Thread(r2, name2);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private static void header(String title) {
        System.out.println("\n------------------------------------------------------------------------------------");
        System.out.println("  " + title);
        System.out.println("------------------------------------------------------------------------------------");
    }

    private static void log(String actor, String msg) {
        System.out.printf("      [%-16s] %s%n", actor, msg);
    }

    private static void logTime(long startNanos, String actor, String msg) {
        long ms = (System.nanoTime() - startNanos) / 1_000_000;
        System.out.printf("      [T+%3d ms | %-12s] %s%n", ms, actor, msg);
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void await(CountDownLatch latch) {
        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try { barrier.await(); } catch (InterruptedException | BrokenBarrierException e) { Thread.currentThread().interrupt(); }
    }
}