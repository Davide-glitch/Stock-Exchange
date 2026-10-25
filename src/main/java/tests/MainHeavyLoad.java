package tests;

import engine.StockExchange;
import model.MarketSnapshot;
import model.Money;
import model.Transaction;
import simulation.BuyerTask;
import simulation.SellerTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HIGH-CONCURRENCY STRESS BENCHMARK: 32 THREADS & 10 TICKERS.
 *
 * Architecture under heavy load:
 *  - 32 Worker Threads in a fixed pool (16 BuyerTask + 16 SellerTask)
 *  - 10 Independent OrderBooks (10 separate ReentrantLocks -> up to 10-way lock parallelism)
 *  - 5,000 new orders per trader = 160,000 new orders (+ ~32,000 cancel/re-price operations)
 *  - 1 Background Monitor Thread reading live MarketSnapshots while trading is in progress
 */
public class MainHeavyLoad {
    private static final List<String> TICKERS = List.of(
            "AAPL", "GOOGL", "MSFT", "NVDA", "AMZN",
            "META", "TSLA",  "AMD",  "INTC", "NFLX"
    );
    private static final long OPENING_PRICE_CENTS = 15_000; // $150.00
    private static final int BUYERS = 16;
    private static final int SELLERS = 16;
    private static final int ORDERS_PER_TRADER = 5_000;     // 32 * 5,000 = 160,000 orders

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int totalThreads = BUYERS + SELLERS;
        int totalOrders = totalThreads * ORDERS_PER_TRADER;

        System.out.println("==========================================================================================");
        System.out.printf("  HEAVY LOAD EXCHANGE SIMULATION: %d THREADS (%d Buyers + %d Sellers) | %d TICKERS%n",
                totalThreads, BUYERS, SELLERS, TICKERS.size());
        System.out.printf("  Workload: %,d new orders (%d per trader) + live concurrent snapshot monitoring%n",
                totalOrders, ORDERS_PER_TRADER);
        System.out.println("==========================================================================================\n");

        StockExchange exchange = new StockExchange(TICKERS, OPENING_PRICE_CENTS);

        // 1. Live market observer (1 scheduled daemon thread reading volatile prices & snapshots concurrently)
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Live-Ticker-Monitor");
            t.setDaemon(true);
            return t;
        });
        monitor.scheduleAtFixedRate(() -> printLiveTape(exchange), 25, 40, TimeUnit.MILLISECONDS);

        // 2. Fixed thread pool of 32 trader threads (16 Buyers + 16 Sellers)
        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);
        List<Future<Integer>> buyerFutures = new ArrayList<>(BUYERS);
        List<Future<Integer>> sellerFutures = new ArrayList<>(SELLERS);

        long startNanos = System.nanoTime();
        for (int i = 1; i <= BUYERS; i++) {
            buyerFutures.add(pool.submit(new BuyerTask("Buyer-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        for (int i = 1; i <= SELLERS; i++) {
            sellerFutures.add(pool.submit(new SellerTask("Seller-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        pool.shutdown();

        // 3. Collect all 32 Future results (blocks until all 160,000 orders finish)
        int sharesToBuy = sumFutures(buyerFutures);
        int sharesToSell = sumFutures(sellerFutures);
        long elapsedNanos = System.nanoTime() - startNanos;
        long elapsedMillis = Math.max(1, elapsedNanos / 1_000_000);

        monitor.shutdownNow();

        // 4. Print detailed per-ticker breakdown, throughput metrics, and global conservation audit
        printFinalReport(exchange, sharesToBuy, sharesToSell, totalOrders, elapsedMillis);
    }

    /** Concurrent reader: proves getLastPriceCents() (volatile) is safely readable during heavy trading. */
    private static void printLiveTape(StockExchange exchange) {
        StringBuilder sb = new StringBuilder("  [LIVE TAPE] ");
        for (int i = 0; i < 5; i++) {
            String t = TICKERS.get(i);
            sb.append(t).append(": ").append(Money.format(exchange.getLastPriceCents(t))).append("  |  ");
        }
        System.out.println(sb);
    }

    private static int sumFutures(List<Future<Integer>> futures) throws InterruptedException, ExecutionException {
        int total = 0;
        for (Future<Integer> future : futures) {
            total += future.get();
        }
        return total;
    }

    private static void printFinalReport(StockExchange exchange, int sharesToBuy, int sharesToSell,
                                         int totalOrders, long elapsedMillis) {
        List<Transaction> trades = exchange.getTradeHistory();
        List<MarketSnapshot> snapshots = exchange.getSnapshots();

        // Group trade count and share volume by ticker
        Map<String, List<Transaction>> tradesByTicker = trades.stream()
                .collect(Collectors.groupingBy(Transaction::ticker));

        System.out.println("\n------------------------------------------------------------------------------------------");
        System.out.println("  FINAL MARKET STATE ACROSS ALL 10 ORDER BOOKS (Each guarded by its own ReentrantLock)");
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.println("  Ticker | Last Price | Best Bid   | Best Ask   | Trades   | Traded Qty | Open Buy | Open Sell");
        System.out.println("  -------+------------+------------+------------+----------+------------+----------+----------");

        for (MarketSnapshot s : snapshots) {
            List<Transaction> tickerTrades = tradesByTicker.getOrDefault(s.ticker(), List.of());
            int tickerTradedQty = tickerTrades.stream().mapToInt(Transaction::quantity).sum();
            System.out.printf("  %-6s | %10s | %10s | %10s | %8d | %10d | %8d | %8d%n",
                    s.ticker(),
                    Money.format(s.lastPriceCents()),
                    s.bestBidCents() == MarketSnapshot.NONE ? "-" : Money.format(s.bestBidCents()),
                    s.bestAskCents() == MarketSnapshot.NONE ? "-" : Money.format(s.bestAskCents()),
                    tickerTrades.size(),
                    tickerTradedQty,
                    s.openBuyShares(),
                    s.openSellShares());
        }

        int totalTradedShares = trades.stream().mapToInt(Transaction::quantity).sum();
        int totalWaitingBuy = snapshots.stream().mapToInt(MarketSnapshot::openBuyShares).sum();
        int totalWaitingSell = snapshots.stream().mapToInt(MarketSnapshot::openSellShares).sum();

        long ordersPerSec = (totalOrders * 1_000L) / elapsedMillis;
        long tradesPerSec = (trades.size() * 1_000L) / elapsedMillis;

        System.out.println("\n------------------------------------------------------------------------------------------");
        System.out.println("  PERFORMANCE & THROUGHPUT METRICS");
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf("  Total Execution Time : %,d ms%n", elapsedMillis);
        System.out.printf("  Active Worker Pool   : %d threads (%d BuyerTask + %d SellerTask)%n",
                BUYERS + SELLERS, BUYERS, SELLERS);
        System.out.printf("  Total Orders Placed  : %,d new orders (~%,d orders/sec)%n", totalOrders, ordersPerSec);
        System.out.printf("  Total Trades Matched : %,d trades     (~%,d trades/sec)%n", trades.size(), tradesPerSec);

        boolean buyOk = (sharesToBuy == totalTradedShares + totalWaitingBuy);
        boolean sellOk = (sharesToSell == totalTradedShares + totalWaitingSell);

        System.out.println("\n------------------------------------------------------------------------------------------");
        System.out.println("  GLOBAL SHARE CONSERVATION AUDIT (Callable<Integer> + Future<Integer>)");
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.printf("  BUY  SIDE : %,9d submitted == %,9d traded + %,8d waiting  -> %s%n",
                sharesToBuy, totalTradedShares, totalWaitingBuy, buyOk ? "OK" : "MISMATCH");
        System.out.printf("  SELL SIDE : %,9d submitted == %,9d traded + %,8d waiting  -> %s%n",
                sharesToSell, totalTradedShares, totalWaitingSell, sellOk ? "OK" : "MISMATCH");
        System.out.println("==========================================================================================");
        System.out.println(buyOk && sellOk
                ? "  FINAL VERDICT: PASS (Zero lost updates across 32 threads and 10 concurrent order books)"
                : "  FINAL VERDICT: FAIL (Data corruption detected)");
        System.out.println("==========================================================================================\n");
    }
}