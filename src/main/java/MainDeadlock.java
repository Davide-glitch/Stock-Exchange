import engine.SettlementEngine;
import model.Account;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MainDeadlock {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Running Faulty Version 2A: Deadlock...");
        System.out.println("Characteristics: CPU drops to ~0%, all threads halt permanently in circular wait.");

        SettlementEngine engine = new SettlementEngine();
        engine.registerAccount(new Account("ACC-A", 5000, 100));
        engine.registerAccount(new Account("ACC-B", 5000, 100));

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Thread 1: Locks A, tries to lock B
        executor.submit(() -> {
            Thread.currentThread().setName("Transfer-A-to-B");
            engine.settleDeadlock("ACC-A", "ACC-B", 200, 5);
        });

        // Thread 2: Locks B, tries to lock A
        executor.submit(() -> {
            Thread.currentThread().setName("Transfer-B-to-A");
            engine.settleDeadlock("ACC-B", "ACC-A", 150, 3);
        });

        executor.shutdown();
        boolean completed = executor.awaitTermination(4, TimeUnit.SECONDS);

        if (!completed) {
            System.err.println("\n[DEADLOCK CONFIRMED] Circular wait detected. System frozen.");
            System.err.println("Run 'jcmd <PID> Thread.print' to inspect the deadlocked locks.");
            System.exit(0);
        }
    }
}