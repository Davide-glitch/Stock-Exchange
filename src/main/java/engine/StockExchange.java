package engine;

import model.MarketSnapshot;
import model.Order;
import model.Transaction;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The exchange: routes every order to the OrderBook of its ticker and keeps the public trade history.
 *
 * CONCURRENCY DESIGN
 *  - There is NO lock in this class. The only locks are the per-ticker ones inside OrderBook.
 *  - `books` is filled once in the constructor and never changed, so any number of threads may read it without
 *    synchronization. (If tickers could be listed while the exchange runs, it would become a ConcurrentHashMap.)
 *  - `history` is a ConcurrentLinkedQueue: lock-free (it uses compare-and-swap), so every book can append to it at the
 *    same time without ever blocking another book. A synchronizedList would be ONE lock shared by all tickers = the
 *    very bottleneck we avoid.
 *  - Trades are appended AFTER the book's lock was released, so no thread ever holds a lock while touching the
 *    history. No nested locks, no deadlock possible.
 */
public class StockExchange {
    private final Map<String, OrderBook> books;
    private final Queue<Transaction> history = new ConcurrentLinkedQueue<>();

    public StockExchange(Collection<String> tickers, long openingPriceCents) {
        Map<String, OrderBook> listed = new HashMap<>();
        for (String ticker : tickers) {
            listed.put(ticker, new OrderBook(ticker, openingPriceCents));
        }
        this.books = Map.copyOf(listed);   // immutable, and safely published through a final field
    }

    /** Offer or request: matches it and records the resulting trades. */
    public void submitOrder(Order order) {
        List<Transaction> trades = bookOf(order.getTicker()).process(order);   // lock taken and released inside
        history.addAll(trades);                                                // lock-free, outside any lock
    }

    /** Withdraws a waiting order; returns the shares that were still open (0 = too late, already traded). */
    public int cancelOrder(Order order) {
        return bookOf(order.getTicker()).cancel(order);
    }

    // ---- information that "everyone has access to" -------------------------------------------------------------

    public long getLastPriceCents(String ticker) {
        return bookOf(ticker).getLastPriceCents();
    }

    /** One snapshot per ticker. Books are visited one after the other: never two locks at once. */
    public List<MarketSnapshot> getSnapshots() {
        return books.values().stream()
                .map(OrderBook::getSnapshot)
                .sorted(Comparator.comparing(MarketSnapshot::ticker))
                .toList();
    }

    public List<Transaction> getTradeHistory() {
        return List.copyOf(history);
    }

    private OrderBook bookOf(String ticker) {
        OrderBook book = books.get(ticker);
        if (book == null) {
            throw new IllegalArgumentException("Unknown ticker: " + ticker);
        }
        return book;
    }
}
