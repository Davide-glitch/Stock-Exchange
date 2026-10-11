import model.Account;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MainStarvation {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Running Starvation Demonstration (Unfair vs Fair ReentrantLock)...");

        runTest(false, "UNFAIR LOCK (Default: Barging / Line-Cutting Allowed)");
        runTest(true,  "FAIR LOCK (ReentrantLock(true): FIFO Queue Enforced)");
    }

    private static void runTest(boolean fair, String label) throws InterruptedException {
        System.out.println("\n=== " + label + " ===");
        Account sharedAccount = new Account("SHARED", 10000, 500, fair);

        int aggressiveCount = 3;
        AtomicInteger[] aggressiveMeals = new AtomicInteger[aggressiveCount];
        for (int i = 0; i < aggressiveCount; i++) aggressiveMeals[i] = new AtomicInteger(0);
        AtomicInteger victimMeals = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(aggressiveCount + 1);
        long endTime = System.currentTimeMillis() + 3000; // 3 second run

        // Aggressive fast threads: barge and repeatedly capture the lock
        for (int i = 0; i < aggressiveCount; i++) {
            final int id = i;
            pool.submit(() -> {
                while (System.currentTimeMillis() < endTime) {
                    sharedAccount.getLock().lock();
                    try {
                        aggressiveMeals[id].incrementAndGet();
                    } finally {
                        sharedAccount.getLock().unlock();
                    }
                }
            });
        }

        // Starved "Victim" thread: sleeps briefly between requests
        pool.submit(() -> {
            while (System.currentTimeMillis() < endTime) {
                sharedAccount.getLock().lock();
                try {
                    victimMeals.incrementAndGet();
                } finally {
                    sharedAccount.getLock().unlock();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        for (int i = 0; i < aggressiveCount; i++) {
            System.out.printf("Aggressive Trader %d operations: %d%n", i, aggressiveMeals[i].get());
        }
        System.out.printf("Starved Trader operations:         %d%n", victimMeals.get());
    }
}