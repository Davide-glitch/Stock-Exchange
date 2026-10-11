package engine;

import model.Order;
import model.Transaction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StockExchange {
    // Per-ticker fine-grained structure: concurrent reads without coarse locking
    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Transaction> history = new ConcurrentLinkedQueue<>();

    public void registerTicker(String ticker) {
        books.putIfAbsent(ticker, new OrderBook(ticker));
    }

    public void submitOrder(Order order) {
        OrderBook book = books.get(order.getTicker());
        if (book == null) {
            throw new IllegalArgumentException("Unknown ticker: " + order.getTicker());
        }
        book.processOrder(order, history::add);
    }

    public boolean cancelOrder(String ticker, long orderId) {
        OrderBook book = books.get(ticker);
        return book != null && book.cancelOrder(orderId);
    }

    public List<Transaction> getTransactionHistory() {
        return List.copyOf(history);
    }

    public OrderBook getOrderBook(String ticker) {
        return books.get(ticker);
    }

    public Map<String, OrderBook> getAllOrderBooks() {
        return Collections.unmodifiableMap(books);
    }
}