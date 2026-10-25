package faulty;

import java.util.ArrayList;
import java.util.List;
import model.Order;
import model.OrderType;
import model.Transaction;

/**
 * FAULTY VERSION #1 (data races / broken consistency): engine.OrderBook WITHOUT its lock.
 * The matching idea is the same, but 16 threads use it at the same time with no synchronization. What goes wrong:
 *
 *  BUG 1  check-then-act: a thread checks "resting.isFilled()" and decides on a quantity; a moment later it acts on that
 *         decision. In between, another thread may have used up the same resting order, so both of them trade shares
 *         that exist only once -> the resting order ends with a NEGATIVE remaining quantity ("sold twice").
 *  BUG 2  lost update: fill() does remainingQuantity -= x, which is read, subtract, write. Two threads can read the same
 *         value and one subtraction is lost -> shares appear or disappear.
 *  BUG 3  ArrayList is not thread-safe: two concurrent add() calls can overwrite each other -> an order silently vanishes
 *         (its shares are lost), leaves null holes in the backing array, or throws an exception.
 *  BUG 4  nextTradeId++ is also read-modify-write -> duplicate trade ids.
 *
 * Thread.yield() is put between the check and the act on purpose. It does not CAUSE the bug (the missing lock does);
 * it only makes the unlucky timing show up on every run instead of once in a million runs.
 *
 * THE FIX is exactly what is missing: hold ONE lock around the whole match (see engine.OrderBook).
 */
public class RacyOrderBook {
    private final List<Order> bids = new ArrayList<>();     // BUG 3
    private final List<Order> asks = new ArrayList<>();     // BUG 3
    private long nextTradeId = 1;                           // BUG 4

    public List<Transaction> process(Order incoming) {
        List<Transaction> trades = new ArrayList<>();
        boolean isBuy = incoming.getType() == OrderType.BUY;
        List<Order> oppositeSide = isBuy ? asks : bids;
        List<Order> ownSide = isBuy ? bids : asks;

        for (int i = 0; i < oppositeSide.size() && !incoming.isFilled(); i++) {
            Order resting = oppositeSide.get(i);
            if (!resting.isFilled() && incoming.canTradeWith(resting)) {                       // BUG 1: the CHECK ...
                int quantity = Math.min(incoming.getRemainingQuantity(), resting.getRemainingQuantity());
                Thread.yield();                                                                // widens the gap between check and act
                incoming.fill(quantity);                                                       // ... the ACT (BUG 2 hides inside fill)
                resting.fill(quantity);
                trades.add(Transaction.between(nextTradeId++, incoming, resting, quantity));   // BUG 4
            }
        }
        if (!incoming.isFilled()) {
            ownSide.add(incoming);                                                             // BUG 3
        }
        return trades;
    }

    /** Shares waiting on one side. Overfilled orders count NEGATIVE, so errors cannot hide. */
    public int openQuantity(OrderType side) {
        int total = 0;
        for (Order order : (side == OrderType.BUY ? bids : asks)) {
            if (order != null) {   // concurrent ArrayList.add() can leave null holes in the internal array
                total += order.getRemainingQuantity();
            }
        }
        return total;
    }

    /** Orders that traded more shares than they had. In a correct book this list is always empty. */
    public List<Order> overfilledOrders() {
        List<Order> overfilled = new ArrayList<>();
        for (Order order : bids) {
            if (order != null && order.getRemainingQuantity() < 0) overfilled.add(order);
        }
        for (Order order : asks) {
            if (order != null && order.getRemainingQuantity() < 0) overfilled.add(order);
        }
        return overfilled;
    }
}