package model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A limit order: "buy (or sell) {quantity} shares of {ticker} at {price} or better".
 *
 * THREAD-SAFETY
 *  - Every field is final EXCEPT remainingQuantity.
 *  - remainingQuantity is deliberately NOT volatile/atomic: it is only read or written while holding the lock of the
 *    OrderBook that owns the order, and that lock already gives mutual exclusion AND visibility.
 *    (Before it is submitted and after it is withdrawn, only one thread can see the order.)
 *  - Ids come from an AtomicLong: many trader threads create orders at the same moment WITHOUT a lock, and a plain
 *    "counter++" there could hand out the same id twice.
 */
public class Order {
    private static final AtomicLong NEXT_ID = new AtomicLong(1);

    private final long id;
    private final String traderId;
    private final String ticker;
    private final OrderType type;
    private final long priceCents;
    private final int initialQuantity;
    private int remainingQuantity;          // guarded by the owning OrderBook's lock

    public Order(String traderId, String ticker, OrderType type, long priceCents, int quantity) {
        this.id = NEXT_ID.getAndIncrement();
        this.traderId = traderId;
        this.ticker = ticker;
        this.type = type;
        this.priceCents = priceCents;
        this.initialQuantity = quantity;
        this.remainingQuantity = quantity;
    }

    /** True if THIS order (the incoming one) is willing to trade at the price of that resting order. */
    public boolean canTradeWith(Order resting) {
        return type == OrderType.BUY
                ? priceCents >= resting.priceCents      // a buyer accepts any offer at or below its bid
                : priceCents <= resting.priceCents;     // a seller accepts any request at or above its ask
    }

    /** Takes {@code quantity} shares out of this order. Only call it while holding the owning book's lock. */
    public void fill(int quantity) {
        remainingQuantity -= quantity;
    }

    public boolean isFilled() {
        return remainingQuantity <= 0;
    }

    public long getId() { return id; }
    public String getTraderId() { return traderId; }
    public String getTicker() { return ticker; }
    public OrderType getType() { return type; }
    public long getPriceCents() { return priceCents; }
    public int getRemainingQuantity() { return remainingQuantity; }

    @Override
    public String toString() {
        return String.format("%s #%d %s %d/%d %s @ %s",
                type, id, traderId, remainingQuantity, initialQuantity, ticker, Money.format(priceCents));
    }
}
