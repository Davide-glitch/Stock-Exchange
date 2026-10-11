import engine.SettlementEngine;
import model.Account;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MainLivelock {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Running Faulty Version 2B: Livelock...");
        System.out.println("Characteristics: CPU pegged at ~100%, threads are active, but zero progress is made.");

        SettlementEngine engine = new SettlementEngine();
        engine.registerAccount(new Account("ACC-A", 5000, 100));
        engine.registerAccount(new Account("ACC-B", 5000, 100));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean stopSignal = new AtomicBoolean(false);

        executor.submit(() -> {
            Thread.currentThread().setName("Livelock-Worker-1");
            engine.settleLivelock("ACC-A", "ACC-B", 100, 2);
        });

        executor.submit(() -> {
            Thread.currentThread().setName("Livelock-Worker-2");
            engine.settleLivelock("ACC-B", "ACC-A", 100, 2);
        });

        // Let the livelock burn CPU for 3 seconds to demonstrate
        Thread.sleep(3000);
        System.out.println("\n[LIVELOCK OBSERVED] 3 seconds elapsed: CPU active, 0 successful transactions.");
        System.out.println("Terminating threads.");
        executor.shutdownNow();
    }
}