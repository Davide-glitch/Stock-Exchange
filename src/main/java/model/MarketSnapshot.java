package model;

/**
 * An immutable picture of one ticker's market at one instant: the information "everyone has access to".
 * Because it is immutable it can be given to any thread; all its fields were read under ONE lock acquisition,
 * so they belong together (the best bid and the open volume describe the same moment).
 */
public record MarketSnapshot(String ticker, long lastPriceCents, long bestBidCents, long bestAskCents,
                             int openBuyShares, int openSellShares) {

    public static final long NONE = -1;     // "there is no bid / no ask right now"

    @Override
    public String toString() {
        return String.format("%-5s last %s | best bid %s | best ask %s | waiting: %d shares to buy, %d to sell",
                ticker, Money.format(lastPriceCents), price(bestBidCents), price(bestAskCents),
                openBuyShares, openSellShares);
    }

    private static String price(long cents) {
        return cents == NONE ? "-" : Money.format(cents);
    }
}
