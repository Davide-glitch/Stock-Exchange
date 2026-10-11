package model;

import java.util.concurrent.atomic.AtomicLong;

public class Order {
    // AtomicLong provides lock-free, atomic ID generation via CPU CAS instructions
    private static final AtomicLong ID_GENERATOR = new AtomicLong(1);

    private final long orderId;
    private final String clientId;
    private final String ticker;
    private final OrderType type;
    private final double price;
    private final int initialQuantity;
    private int remainingQuantity;
    private final long timestamp;

    public Order(String clientId, String ticker, OrderType type, double price, int quantity) {
        this.orderId = ID_GENERATOR.getAndIncrement();
        this.clientId = clientId;
        this.ticker = ticker;
        this.type = type;
        this.price = price;
        this.initialQuantity = quantity;
        this.remainingQuantity = quantity;
        this.timestamp = System.nanoTime();
    }

    public long getOrderId() { return orderId; }
    public String getClientId() { return clientId; }
    public String getTicker() { return ticker; }
    public OrderType getType() { return type; }
    public double getPrice() { return price; }
    public int getInitialQuantity() { return initialQuantity; }
    public int getRemainingQuantity() { return remainingQuantity; }
    public long getTimestamp() { return timestamp; }

    public void deductQuantity(int amount) {
        this.remainingQuantity -= amount;
    }

    public boolean isFilled() {
        return this.remainingQuantity <= 0;
    }

    @Override
    public String toString() {
        return String.format("[%s #%d] %s %d/%d %s @ $%.2f",
                type, orderId, clientId, remainingQuantity, initialQuantity, ticker, price);
    }
}