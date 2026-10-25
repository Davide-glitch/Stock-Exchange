package simulation;

import engine.StockExchange;
import model.Order;
import model.OrderType;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One trader = one task that runs on one thread of the pool.
 *
 * Runnable vs Callable: this is a Callable<Integer> because the main thread needs a RESULT back (how many shares this
 * trader submitted, used by the audit). A Runnable returns nothing: fine for "fire and forget", not for this.
 * Collecting the result through a Future also means NO counter is shared between the 16 threads.
 *
 * The role (buyer or seller) is fixed per task: see BuyerTask and SellerTask.
 *
 * The trader's algorithm uses the exchange's common information: it prices every order around the LAST TRADE PRICE,
 * and sometimes re-prices its previous order (= modifying a request/offer) when it is still waiting in the book.
 *
 * Nothing here runs under a lock. The only lock is taken inside exchange.submitOrder()/cancelOrder(), and only for the
 * short matching step. Creating orders and generating random numbers happens outside any critical section.
 */
public abstract class TraderTask implements Callable<Integer> {
    private static final int MAX_QUANTITY = 10;
    private static final int REPRICE_ONE_IN = 5;      // on average every 5th round the trader also re-prices an order

    private final String traderId;
    private final OrderType side;
    private final StockExchange exchange;
    private final List<String> tickers;
    private final int ordersToPlace;

    protected TraderTask(String traderId, OrderType side, StockExchange exchange,
                         List<String> tickers, int ordersToPlace) {
        this.traderId = traderId;
        this.side = side;
        this.exchange = exchange;
        this.tickers = tickers;
        this.ordersToPlace = ordersToPlace;
    }

    @Override
    public Integer call() {
        ThreadLocalRandom random = ThreadLocalRandom.current();   // one generator per thread: nothing shared, no waiting
        int sharesSubmitted = 0;
        Order previous = null;

        for (int i = 0; i < ordersToPlace; i++) {
            // 1) Modify: sometimes withdraw my previous order and quote it again at today's price.
            if (previous != null && random.nextInt(REPRICE_ONE_IN) == 0) {
                int stillOpen = exchange.cancelOrder(previous);       // 0 = it was completely traded meanwhile: nothing to do
                if (stillOpen > 0) {
                    // Same shares, new price. Shares are only moved, not created, so the audit is not affected.
                    exchange.submitOrder(newOrder(previous.getTicker(), stillOpen, random));
                }
            }

            // 2) Place a brand-new order.
            String ticker = tickers.get(random.nextInt(tickers.size()));
            int quantity = 1 + random.nextInt(MAX_QUANTITY);
            previous = newOrder(ticker, quantity, random);
            exchange.submitOrder(previous);
            sharesSubmitted += quantity;
        }
        return sharesSubmitted;
    }

    /** An order priced around the market: the last trade price, give or take up to 1 %. */
    private Order newOrder(String ticker, int quantity, ThreadLocalRandom random) {
        long lastPrice = exchange.getLastPriceCents(ticker);                 // common information, read without a lock
        long price = lastPrice + lastPrice * random.nextInt(-100, 101) / 10_000;
        return new Order(traderId, ticker, side, Math.max(1, price), quantity);
    }
}
