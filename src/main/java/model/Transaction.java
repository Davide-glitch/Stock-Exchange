package model;

/**
 * One executed trade. A record is immutable, so it can be handed from thread to thread with no synchronization at all.
 * The id is unique per ticker (each OrderBook numbers its own trades).
 */
public record Transaction(long id, String ticker, String buyerId, String sellerId, long priceCents, int quantity) {

    /** The trade between an incoming order and an order already waiting in the book. It happens at the WAITING order's price. */
    public static Transaction between(long id, Order incoming, Order resting, int quantity) {
        Order buy = (incoming.getType() == OrderType.BUY) ? incoming : resting;
        Order sell = (buy == incoming) ? resting : incoming;
        return new Transaction(id, incoming.getTicker(), buy.getTraderId(), sell.getTraderId(),
                resting.getPriceCents(), quantity);
    }

    @Override
    public String toString() {
        return String.format("%s#%d: %d share(s) @ %s  (%s buys from %s)",
                ticker, id, quantity, Money.format(priceCents), buyerId, sellerId);
    }
}
