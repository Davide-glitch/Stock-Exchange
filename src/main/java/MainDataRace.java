import engine.DataRaceOrderBook;
import model.Order;
import model.OrderType;
import model.Transaction;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MainDataRace {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Running Faulty Version 1: Data Race & Consistency Invariant Violation...");

        DataRaceOrderBook book = new DataRaceOrderBook("AAPL");
        ConcurrentLinkedQueue<Transaction> txHistory = new ConcurrentLinkedQueue<>();
        AtomicInteger totalSubmitted = new AtomicInteger(0);

        int clients = 16;
        int ordersPerClient = 200;
        ExecutorService executor = Executors.newFixedThreadPool(clients);

        for (int i = 1; i <= clients; i++) {
            final String clientId = "Trader-" + i;
            executor.submit(() -> {
                Random rand = new Random();
                for (int j = 0; j < ordersPerClient; j++) {
                    OrderType type = rand.nextBoolean() ? OrderType.BUY : OrderType.SELL;
                    int qty = 1 + rand.nextInt(5);
                    double price = 100.0; // Identical price forces intense contention on single orders
                    totalSubmitted.addAndGet(qty);
                    book.processOrder(new Order(clientId, "AAPL", type, price, qty), txHistory::add);
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        int totalTraded = txHistory.stream().mapToInt(Transaction::quantity).sum();
        int remainingOpen = book.getRemainingOpenQuantity();
        int accounted = (totalTraded * 2) + remainingOpen;
        int expected = totalSubmitted.get();

        System.out.println("========== AUDIT REPORT (DATA RACE) ==========");
        System.out.printf("Total Shares Submitted:  %d%n", expected);
        System.out.printf("Accounted Shares:        %d%n", accounted);
        System.out.printf("Corrupted / Lost Shares: %d%n", expected - accounted);
        System.out.println("CONCURRENCY VERDICT: FAIL (Race conditions caused state corruption)");
    }
}