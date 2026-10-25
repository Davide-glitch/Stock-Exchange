package faulty;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * FAULTY VERSION #2a: DEADLOCK, followed by its fix.
 *
 * Scenario: settling a trade changes two accounts, so it needs two locks. Two trades run at the same time in OPPOSITE
 * directions: "A buys from B" and "B buys from A".
 *
 * A deadlock needs all four Coffman conditions:
 *   1. mutual exclusion (a lock has one owner)      2. hold and wait (keep my lock while asking for the next)
 *   3. no preemption (nobody can take a lock away)  4. circular wait (A waits for B, B waits for A)
 *
 * Faulty: each thread locks "its" account first (the buyer), then the other one -> opposite order -> circular wait.
 * Fix:    all threads lock in the SAME global order (smaller account id first) -> a cycle is impossible.
 *         This removes condition 4.
 * Fix B:  tryLock with a timeout: if the second lock does not come, give up the first, wait a random time, retry.
 *         This removes "hold and wait" (the thread does not wait forever while holding a lock).
 *         A third cure is one single lock for both accounts (but that is coarse-grained).
 *
 * Symptom: CPU drops to ~0 %, the threads stay BLOCKED forever. Here the JVM itself proves it with
 * ThreadMXBean (the same information `jstack` / `jcmd <pid> Thread.print` would show).
 */
public class MainDeadlock {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1) FAULTY: lock the buyer first, then the seller ===");
        runFaulty();
        System.out.println("\n=== 2) FIX A: always lock the account with the smaller id first ===");
        runFixed(MainDeadlock::settleOrdered);
        System.out.println("\n=== 3) FIX B: tryLock with a timeout, then back off and retry ===");
        runFixed(MainDeadlock::settleWithTimeout);
    }

    // ---- the two versions of the settlement -------------------------------------------------------------------

    /** FAULTY: the lock order depends on who is buying, so two opposite trades lock in opposite order. */
    private static void settleFaulty(Account buyer, Account seller, long cash, int shares) {
        buyer.getLock().lock();
        try {
            pause(100);                         // "work" between the two locks (a risk check...); makes the bad timing certain
            seller.getLock().lock();            // <-- each thread waits here for the lock the other one holds
            try {
                buyer.settleBuy(seller, cash, shares);
            } finally {
                seller.getLock().unlock();
            }
        } finally {
            buyer.getLock().unlock();
        }
    }

    /** FIXED: the lock order depends only on the account ids, the same for every thread. */
    private static void settleOrdered(Account buyer, Account seller, long cash, int shares) {
        Account first = buyer.getId().compareTo(seller.getId()) < 0 ? buyer : seller;
        Account second = (first == buyer) ? seller : buyer;

        first.getLock().lock();
        try {
            pause(100);                         // exactly the same delay: ordering is safe even with the worst timing
            second.getLock().lock();
            try {
                buyer.settleBuy(seller, cash, shares);
            } finally {
                second.getLock().unlock();
            }
        } finally {
            first.getLock().unlock();
        }
    }

    /** FIX B: do not wait forever. If the second lock is not free within 200 ms, release the first and retry later. */
    private static void settleWithTimeout(Account buyer, Account seller, long cash, int shares) {
        try {
            while (true) {
                buyer.getLock().lock();
                try {
                    pause(100);                                              // same delay as the faulty version
                    if (seller.getLock().tryLock(200, TimeUnit.MILLISECONDS)) {
                        try {
                            buyer.settleBuy(seller, cash, shares);
                            return;
                        } finally {
                            seller.getLock().unlock();
                        }
                    }
                } finally {
                    buyer.getLock().unlock();                                // give up the first lock: no more circular wait
                }
                pause(ThreadLocalRandom.current().nextInt(10, 60));         // random pause so the retries do not collide again
            }
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }

    /** One settlement strategy; lets the same experiment run with different fixes. */
    private interface Settlement {
        void settle(Account buyer, Account seller, long cash, int shares);
    }

    // ---- the experiments ----------------------------------------------------------------------------------

    private static void runFaulty() throws InterruptedException {
        Account a = new Account("ACC-A", 500_000, 100);
        Account b = new Account("ACC-B", 500_000, 100);

        // Daemon threads: these two never finish, and they must not keep the JVM alive after main() ends.
        ExecutorService pool = Executors.newFixedThreadPool(2, task -> {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        });
        pool.submit(() -> {
            Thread.currentThread().setName("A-buys-from-B");
            settleFaulty(a, b, 20_000, 5);
        });
        pool.submit(() -> {
            Thread.currentThread().setName("B-buys-from-A");
            settleFaulty(b, a, 15_000, 3);
        });

        ThreadMXBean jvm = ManagementFactory.getThreadMXBean();
        long[] deadlocked = null;
        for (int i = 0; i < 50 && deadlocked == null; i++) {     // look for up to 5 seconds
            Thread.sleep(100);
            deadlocked = jvm.findDeadlockedThreads();            // finds cycles on monitors AND on ReentrantLocks
        }

        if (deadlocked == null) {
            System.out.println("No deadlock detected (unexpected).");
            return;
        }
        System.out.println("DEADLOCK confirmed by the JVM (circular wait):");
        for (ThreadInfo thread : jvm.getThreadInfo(deadlocked)) {
            System.out.printf("  %-14s (%s) wants %s%n  %-14s ... which is held by %s%n",
                    thread.getThreadName(), thread.getThreadState(), thread.getLockName(), "", thread.getLockOwnerName());
        }
    }

    private static void runFixed(Settlement settlement) throws InterruptedException {
        Account a = new Account("ACC-A", 500_000, 100);
        Account b = new Account("ACC-B", 500_000, 100);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.submit(() -> settlement.settle(a, b, 20_000, 5));
        pool.submit(() -> settlement.settle(b, a, 15_000, 3));
        pool.shutdown();

        boolean finished = pool.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println(finished ? "Both settlements finished, no deadlock." : "Still running (unexpected).");
        System.out.println("  " + a);
        System.out.println("  " + b);
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
