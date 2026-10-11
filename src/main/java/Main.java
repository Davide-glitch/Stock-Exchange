import engine.OrderBook;
import engine.StockExchange;
import model.Transaction;
import simulation.Trader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Running Correct Concurrent Stock Exchange...");

        StockExchange exchange = new StockExchange();
        List<String> tickers = List.of("AAPL", "GOOGL", "MSFT");
        tickers.forEach(exchange::registerTicker);

        int clientCount = 16;
        int operationsPerClient = 250;
        AtomicInteger totalSharesSubmitted = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(clientCount);

        long startTime = System.currentTimeMillis();
        for (int i = 1; i <= clientCount; i++) {
            executor.submit(new Trader(
                    "Trader-" + i,
                    exchange,
                    tickers,
                    operationsPerClient,
                    totalSharesSubmitted
            ));
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.printf("Finished in %d ms%n", elapsedTime);

        List<Transaction> transactions = exchange.getTransactionHistory();
        int totalTradedShares = transactions.stream().mapToInt(Transaction::quantity).sum();
        int totalOpenShares = exchange.getAllOrderBooks().values().stream()
                .mapToInt(OrderBook::getRemainingOpenQuantity)
                .sum();

        int totalSubmitted = totalSharesSubmitted.get();
        int accountedShares = (totalTradedShares * 2) + totalOpenShares;

        System.out.println("========== AUDIT REPORT ==========");
        System.out.printf("Total Orders Placed (Volume): %d shares%n", totalSubmitted);
        System.out.printf("Transactions Executed:        %d deals (%d shares transferred)%n",
                transactions.size(), totalTradedShares);
        System.out.printf("Remaining Open in Books:      %d shares%n", totalOpenShares);
        System.out.printf("Formula: (Traded * 2) + Open = %d%n", accountedShares);
        System.out.println("==================================");

        if (totalSubmitted == accountedShares) {
            System.out.println("CONCURRENCY VERDICT: PASS (Zero data races, exact balance preserved)");
        } else {
            System.err.printf("CONCURRENCY VERDICT: FAIL! Discrepancy: %d shares lost or duplicated%n",
                    totalSubmitted - accountedShares);
        }
    }
}