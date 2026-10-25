package faulty;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FAULTY VERSION #2b: LIVELOCK, followed by its fix.
 *
 * Same scenario as the deadlock ("A buys from B" and "B buys from A"), but now the threads are POLITE: they use tryLock,
 * and when they cannot get the second lock they release the first one and try again. Nobody is blocked and the threads
 * keep running, but they keep reacting to each other in lockstep, so NO settlement ever completes.
 *
 * Deadlock vs livelock: in a deadlock the threads are blocked (waiting, no activity); in a livelock they are active
 * (retrying) yet make no progress.
 *
 * Faulty: both retry immediately and in perfect symmetry.
 * Fix:    after a failed attempt each thread waits a RANDOM time, which breaks the symmetry; one of them gets in first.
 *
 * Note on the CyclicBarrier in the faulty version: it is a test harness, not part of the idea. It forces the unlucky
 * timing (both threads hold their first lock before either tries the second) so the demo is reproducible. In a real
 * system that symmetry happens by accident.
 */
public class MainLivelock {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1) FAULTY: polite retries without randomness ===");
        runFaulty();
        System.out.println("\n=== 2) FIXED: random back-off (3 rounds) ===");
        for (int round = 1; round <= 3; round++) {
            runFixed(round);
        }
    }

    // ---- the two versions of the settlement -------------------------------------------------------------------

    /** FAULTY: tryLock, and on failure release and retry at once. Both threads do exactly the same thing at the same time. */
    private static void settlePolitely(Account buyer, Account seller, CyclicBarrier sameTiming,
                                       AtomicInteger attempts, AtomicInteger settled,
                                       long cash, int shares) {
        try {
            while (true) {
                attempts.incrementAndGet();
                if (buyer.getLock().tryLock()) {
                    try {
                        sameTiming.await();                       // harness: both threads now hold their first lock
                        if (seller.getLock().tryLock()) {         // never succeeds: the other thread holds it
                            try {
                                buyer.settleBuy(seller, cash, shares);
                                settled.incrementAndGet();
                                return;
                            } finally {
                                seller.getLock().unlock();
                            }
                        }
                        sameTiming.await();                       // harness: both have failed before anybody lets go
                    } finally {
                        buyer.getLock().unlock();                 // "after you!": release the first lock and retry immediately
                    }
                }
            }
        } catch (InterruptedException | BrokenBarrierException stopped) {
            // the demo was stopped with shutdownNow()
        }
    }

    /** FIXED: same tryLock logic, plus a random pause after every failure. */
    private static void settleWithBackoff(Account buyer, Account seller, AtomicInteger attempts,
                                          long cash, int shares) {
        try {
            while (true) {
                attempts.incrementAndGet();
                if (buyer.getLock().tryLock()) {
                    try {
                        Thread.sleep(10);                         // "work" between the two locks: makes a first collision likely
                        if (seller.getLock().tryLock()) {
                            try {
                                buyer.settleBuy(seller, cash, shares);
                                return;
                            } finally {
                                seller.getLock().unlock();
                            }
                        }
                    } finally {
                        buyer.getLock().unlock();
                    }
                }
                Thread.sleep(ThreadLocalRandom.current().nextInt(5, 30));   // <-- the fix: random, so the threads drift apart
            }
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- the two experiments ----------------------------------------------------------------------------------

    private static void runFaulty() throws InterruptedException {
        Account a = new Account("ACC-A", 500_000, 100);
        Account b = new Account("ACC-B", 500_000, 100);
        CyclicBarrier sameTiming = new CyclicBarrier(2);
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger settled = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.submit(() -> settlePolitely(a, b, sameTiming, attempts, settled, 20_000, 5));
        pool.submit(() -> settlePolitely(b, a, sameTiming, attempts, settled, 15_000, 3));

        Thread.sleep(2_000);
        System.out.printf("LIVELOCK after 2 s: %d attempts, %d settlements completed. Threads active, zero progress.%n",
                attempts.get(), settled.get());
        pool.shutdownNow();                                       // interrupt both workers
        pool.awaitTermination(2, TimeUnit.SECONDS);
    }

    private static void runFixed(int round) throws InterruptedException {
        Account a = new Account("ACC-A", 500_000, 100);
        Account b = new Account("ACC-B", 500_000, 100);
        AtomicInteger attempts = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.submit(() -> settleWithBackoff(a, b, attempts, 20_000, 5));
        pool.submit(() -> settleWithBackoff(b, a, attempts, 15_000, 3));
        pool.shutdown();

        boolean finished = pool.awaitTermination(5, TimeUnit.SECONDS);
        System.out.printf("  round %d: %s after %d attempts in total | %s | %s%n", round,
                finished ? "both settlements completed" : "NOT finished (unexpected)", attempts.get(), a, b);
    }
}
