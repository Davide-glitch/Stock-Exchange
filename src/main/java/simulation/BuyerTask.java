package simulation;

import engine.StockExchange;
import model.OrderType;

import java.util.List;

/** Buyer role: this thread ONLY places BUY orders (requests). */
public class BuyerTask extends TraderTask {
    public BuyerTask(String traderId, StockExchange exchange, List<String> tickers, int ordersToPlace) {
        super(traderId, OrderType.BUY, exchange, tickers, ordersToPlace);
    }
}
