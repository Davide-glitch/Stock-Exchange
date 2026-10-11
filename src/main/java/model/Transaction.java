public record Transaction(
        long transactionId,
        String ticker,
        String buyerId,
        String sellerId,
        long buyOrderId,
        long sellOrderId,
        double executionPrice,
        int quantity,
        long timestamp
) {
    @Override
    public String toString() {
        return String.format("TX #%d | %s | %d shares @ $%.2f | Buyer: %s (#%d) <-> Seller: %s (#%d)",
                transactionId, ticker, quantity, executionPrice, buyerId, buyOrderId, sellerId, sellOrderId);
    }
}