import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class OrderBook {
    private final String ticker;
    private final ReentrantLock lock = new ReentrantLock();
    private static final AtomicLong TX_ID_GENERATOR = new AtomicLong(1);

    private final PriorityQueue<Order> bids = new PriorityQueue<>(
            Comparator.comparingDouble(Order::getPrice).reversed()
                    .thenComparingLong(Order::getTimestamp)
    );

    private final PriorityQueue<Order> asks = new PriorityQueue<>(
            Comparator.comparingDouble(Order::getPrice)
                    .thenComparingLong(Order::getTimestamp)
    );

    public OrderBook(String ticker) {
        this.ticker = ticker;
    }

    public void processOrder(Order incomingOrder, Consumer<Transaction> transactionLogger) {
        lock.lock();
        try {
            if (incomingOrder.getType() == OrderType.BUY) {
                matchBuyOrder(incomingOrder, transactionLogger);
            } else {
                matchSellOrder(incomingOrder, transactionLogger);
            }
        } finally {
            lock.unlock();
        }
    }

    private void matchBuyOrder(Order buyOrder, Consumer<Transaction> transactionLogger) {
        while (!asks.isEmpty() && !buyOrder.isFilled()) {
            Order bestAsk = asks.peek();

            if (buyOrder.getPrice() < bestAsk.getPrice()) {
                break;
            }

            int matchQuantity = Math.min(buyOrder.getRemainingQuantity(), bestAsk.getRemainingQuantity());
            double executionPrice = bestAsk.getPrice();

            buyOrder.deductQuantity(matchQuantity);
            bestAsk.deductQuantity(matchQuantity);

            Transaction tx = new Transaction(
                    TX_ID_GENERATOR.getAndIncrement(),
                    ticker,
                    buyOrder.getClientId(),
                    bestAsk.getClientId(),
                    buyOrder.getOrderId(),
                    bestAsk.getOrderId(),
                    executionPrice,
                    matchQuantity,
                    System.currentTimeMillis()
            );
            transactionLogger.accept(tx);

            if (bestAsk.isFilled()) {
                asks.poll();
            }
        }

        if (!buyOrder.isFilled()) {
            bids.add(buyOrder);
        }
    }

    private void matchSellOrder(Order sellOrder, Consumer<Transaction> transactionLogger) {
        while (!bids.isEmpty() && !sellOrder.isFilled()) {
            Order bestBid = bids.peek();

            if (sellOrder.getPrice() > bestBid.getPrice()) {
                break;
            }

            int matchQuantity = Math.min(sellOrder.getRemainingQuantity(), bestBid.getRemainingQuantity());
            double executionPrice = bestBid.getPrice();

            sellOrder.deductQuantity(matchQuantity);
            bestBid.deductQuantity(matchQuantity);

            Transaction tx = new Transaction(
                    TX_ID_GENERATOR.getAndIncrement(),
                    ticker,
                    bestBid.getClientId(),
                    sellOrder.getClientId(),
                    bestBid.getOrderId(),
                    sellOrder.getOrderId(),
                    executionPrice,
                    matchQuantity,
                    System.currentTimeMillis()
            );
            transactionLogger.accept(tx);

            if (bestBid.isFilled()) {
                bids.poll();
            }
        }

        if (!sellOrder.isFilled()) {
            asks.add(sellOrder);
        }
    }

    public boolean cancelOrder(long orderId) {
        lock.lock();
        try {
            boolean removed = bids.removeIf(order -> order.getOrderId() == orderId);
            if (!removed) {
                removed = asks.removeIf(order -> order.getOrderId() == orderId);
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public int getRemainingOpenQuantity() {
        lock.lock();
        try {
            int total = 0;
            for (Order o : bids) total += o.getRemainingQuantity();
            for (Order o : asks) total += o.getRemainingQuantity();
            return total;
        } finally {
            lock.unlock();
        }
    }

    public List<Order> getBidsSnapshot() {
        lock.lock();
        try {
            return new ArrayList<>(bids);
        } finally {
            lock.unlock();
        }
    }

    public List<Order> getAsksSnapshot() {
        lock.lock();
        try {
            return new ArrayList<>(asks);
        } finally {
            lock.unlock();
        }
    }
}