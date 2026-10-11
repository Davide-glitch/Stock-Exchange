package engine;

import model.Order;
import model.OrderType;
import model.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

// Demonstrates lost updates and broken state: unsynchronized check-then-act critical sections
public class DataRaceOrderBook {
    private final String ticker;
    private static final AtomicLong TX_ID_GENERATOR = new AtomicLong(1);

    private final List<Order> bids = new ArrayList<>();
    private final List<Order> asks = new ArrayList<>();

    public DataRaceOrderBook(String ticker) {
        this.ticker = ticker;
    }

    public void processOrder(Order incomingOrder, Consumer<Transaction> logger) {
        if (incomingOrder.getType() == OrderType.BUY) {
            // Race condition: multiple threads match against the same asks simultaneously
            for (int i = 0; i < asks.size() && !incomingOrder.isFilled(); i++) {
                Order ask = asks.get(i);
                if (incomingOrder.getPrice() >= ask.getPrice() && !ask.isFilled()) {
                    int qty = Math.min(incomingOrder.getRemainingQuantity(), ask.getRemainingQuantity());

                    // Thread.yield() forces thread switching within the critical section
                    Thread.yield();

                    incomingOrder.deductQuantity(qty);
                    ask.deductQuantity(qty);

                    logger.accept(new Transaction(
                            TX_ID_GENERATOR.getAndIncrement(), ticker,
                            incomingOrder.getClientId(), ask.getClientId(),
                            incomingOrder.getOrderId(), ask.getOrderId(),
                            ask.getPrice(), qty, System.currentTimeMillis()
                    ));
                }
            }
            if (!incomingOrder.isFilled()) {
                bids.add(incomingOrder);
            }
        } else {
            for (int i = 0; i < bids.size() && !incomingOrder.isFilled(); i++) {
                Order bid = bids.get(i);
                if (incomingOrder.getPrice() <= bid.getPrice() && !bid.isFilled()) {
                    int qty = Math.min(incomingOrder.getRemainingQuantity(), bid.getRemainingQuantity());

                    Thread.yield();

                    incomingOrder.deductQuantity(qty);
                    bid.deductQuantity(qty);

                    logger.accept(new Transaction(
                            TX_ID_GENERATOR.getAndIncrement(), ticker,
                            bid.getClientId(), incomingOrder.getClientId(),
                            bid.getOrderId(), incomingOrder.getOrderId(),
                            bid.getPrice(), qty, System.currentTimeMillis()
                    ));
                }
            }
            if (!incomingOrder.isFilled()) {
                asks.add(incomingOrder);
            }
        }
    }

    public int getRemainingOpenQuantity() {
        int total = 0;
        for (Order o : bids) total += Math.max(0, o.getRemainingQuantity());
        for (Order o : asks) total += Math.max(0, o.getRemainingQuantity());
        return total;
    }
}