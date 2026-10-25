import engine.StockExchange;
import model.MarketSnapshot;
import model.Transaction;
import simulation.BuyerTask;
import simulation.SellerTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The CORRECT concurrent stock exchange: 8 buyer threads and 8 seller threads trade 3 tickers at the same time.
 *
 * Architecture:
 *   Main --> ExecutorService (fixed pool, 16 threads)
 *              |-- 8 x BuyerTask  --.
 *              '-- 8 x SellerTask --+--> StockExchange.submitOrder(order)
 *                                          |--> OrderBook AAPL   (own lock) --.
 *                                          |--> OrderBook GOOGL  (own lock) --+--> trades --> history (lock-free queue)
 *                                          '--> OrderBook MSFT   (own lock) --'
 *
 * How we know it is correct (the AUDIT, done after all threads finished):
 *   a trade takes the same number of shares from one buy order and one sell order, so for each side
 *       submitted shares == traded shares + shares still waiting in the books
 *   A lost update, a duplicated trade or a lost order breaks one of these two equations.
 */
public class Main {
    private static final List<String> TICKERS = List.of("AAPL", "GOOGL", "MSFT");
    private static final long OPENING_PRICE_CENTS = 10_000;     // $100.00
    private static final int BUYERS = 8;
    private static final int SELLERS = 8;
    private static final int ORDERS_PER_TRADER = 2_000;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        StockExchange exchange = new StockExchange(TICKERS, OPENING_PRICE_CENTS);

        // Executor framework: a FIXED pool. At most 16 threads exist, however many orders are placed
        // (we never start one thread per order). Tasks beyond 16 would wait in the pool's internal queue.
        ExecutorService pool = Executors.newFixedThreadPool(BUYERS + SELLERS);

        // Each trader is a Callable: its Future will hold "how many shares did this trader submit".
        List<Future<Integer>> buyerResults = new ArrayList<>();
        List<Future<Integer>> sellerResults = new ArrayList<>();

        long start = System.nanoTime();
        for (int i = 1; i <= BUYERS; i++) {
            buyerResults.add(pool.submit(new BuyerTask("Buyer-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        for (int i = 1; i <= SELLERS; i++) {
            sellerResults.add(pool.submit(new SellerTask("Seller-" + i, exchange, TICKERS, ORDERS_PER_TRADER)));
        }
        pool.shutdown();    // accept no new tasks; the running ones finish and then the threads exit (the JVM can stop)

        int sharesToBuy = total(buyerResults);      // Future.get() waits until that trader is done
        int sharesToSell = total(sellerResults);    // (and rethrows any exception the trader died with)
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        report(exchange, sharesToBuy, sharesToSell, elapsedMillis);
    }

    private static int total(List<Future<Integer>> results) throws InterruptedException, ExecutionException {
        int total = 0;
        for (Future<Integer> result : results) {
            total += result.get();
        }
        return total;
    }

    /** Reads only public information, exactly what any outside observer could read. */
    private static void report(StockExchange exchange, int sharesToBuy, int sharesToSell, long elapsedMillis) {
        List<Transaction> trades = exchange.getTradeHistory();
        List<MarketSnapshot> markets = exchange.getSnapshots();

        int tradedShares = trades.stream().mapToInt(Transaction::quantity).sum();
        int waitingToBuy = markets.stream().mapToInt(MarketSnapshot::openBuyShares).sum();
        int waitingToSell = markets.stream().mapToInt(MarketSnapshot::openSellShares).sum();

        System.out.printf("Finished in %d ms: %d buyers + %d sellers, %d new orders each, %d trades%n%n",
                elapsedMillis, BUYERS, SELLERS, ORDERS_PER_TRADER, trades.size());

        System.out.println("Market state:");
        markets.forEach(market -> System.out.println("  " + market));

        System.out.println("\nFirst trades:");
        trades.stream().limit(3).forEach(trade -> System.out.println("  " + trade));

        boolean buySideOk = sharesToBuy == tradedShares + waitingToBuy;
        boolean sellSideOk = sharesToSell == tradedShares + waitingToSell;
        System.out.println("\nAUDIT: every submitted share is either traded or still waiting");
        System.out.printf("  BUY  side: %6d submitted = %6d traded + %6d waiting  -> %s%n",
                sharesToBuy, tradedShares, waitingToBuy, buySideOk ? "OK" : "MISMATCH");
        System.out.printf("  SELL side: %6d submitted = %6d traded + %6d waiting  -> %s%n",
                sharesToSell, tradedShares, waitingToSell, sellSideOk ? "OK" : "MISMATCH");
        System.out.println(buySideOk && sellSideOk
                ? "VERDICT: PASS (no lost or duplicated shares)"
                : "VERDICT: FAIL (shares were lost or duplicated)");
    }
}
