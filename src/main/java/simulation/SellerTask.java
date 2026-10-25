package simulation;

import engine.StockExchange;
import model.OrderType;

import java.util.List;

/** Seller role: this thread ONLY places SELL orders (offers). */
public class SellerTask extends TraderTask {
    public SellerTask(String traderId, StockExchange exchange, List<String> tickers, int ordersToPlace) {
        super(traderId, OrderType.SELL, exchange, tickers, ordersToPlace);
    }
}
