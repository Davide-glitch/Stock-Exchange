import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Trader implements Runnable {
    private final String clientId;
    private final StockExchange exchange;
    private final List<String> tickers;
    private final int operationsCount;
    private final AtomicInteger totalSharesSubmitted;
    private final Random random = new Random();

    public Trader(String clientId, StockExchange exchange, List<String> tickers,
                  int operationsCount, AtomicInteger totalSharesSubmitted) {
        this.clientId = clientId;
        this.exchange = exchange;
        this.tickers = tickers;
        this.operationsCount = operationsCount;
        this.totalSharesSubmitted = totalSharesSubmitted;
    }

    @Override
    public void run() {
        for (int i = 0; i < operationsCount; i++) {
            String ticker = tickers.get(random.nextInt(tickers.size()));
            OrderType type = random.nextBoolean() ? OrderType.BUY : OrderType.SELL;

            double price = Math.round((95.0 + random.nextDouble() * 10.0) * 100.0) / 100.0;
            int quantity = 1 + random.nextInt(10);

            Order order = new Order(clientId, ticker, type, price, quantity);
            totalSharesSubmitted.addAndGet(quantity);

            exchange.submitOrder(order);

            try {
                Thread.sleep(random.nextInt(4));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}