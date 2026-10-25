package faulty;

import engine.OrderBook;
import model.MarketSnapshot;
import model.Order;
import model.OrderType;
import model.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * FAULTY VERSION #1: DATA RACE and broken data consistency (see RacyOrderBook for the four bugs).
 *
 * Definitions to remember:
 *  - Data race: two threads access the same variable, at least one of them writes, and nothing orders the accesses
 *    (no lock, no volatile, no atomic).
 *  - Race condition: the result depends on the timing of the threads. Check-then-act and read-modify-write are the
 *    two classic shapes.
 *
 * The experiment: the SAME 16 traders (8 buyers + 8 sellers) hit ONE ticker at the SAME price, so every order tries to
 * trade with the same few waiting orders. They run three rounds against the broken book (RacyOrderBook) and three
 * rounds against the real book (engine.OrderBook, with its lock). Only the book differs. The trade history is a
 * thread-safe queue in both cases, so the book is the ONLY thing that can corrupt the data.
 */
public class MainDataRace {
    private static final int BUYERS = 8;
    private static final int SELLERS = 8;
    private static final int ORDERS_PER_TRADER = 500;
    private static final int ROUNDS = 3;

    /** What the experiment needs from a book, so the same traders can run against either implementation. */
    private interface Book {
        List<Transaction> process(Order order);
        int waiting(OrderType side);
        int overfilled();                   // orders that traded more shares than they had
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.out.println("Same 16 traders, same orders. Only the order book differs.\n");

        System.out.println("A) FAULTY book: no lock (RacyOrderBook)");
        for (int round = 1; round <= ROUNDS; round++) {
            print(round, run(MainDataRace::racyBook));
        }

        System.out.println("\nB) CORRECT book: engine.OrderBook (one ReentrantLock around the whole match)");
        for (int round = 1; round <= ROUNDS; round++) {
            print(round, run(MainDataRace::lockedBook));
        }

        System.out.println("\nColumns: overfilled = orders that traded more shares than they had (sold twice);");
        System.out.println("balanced = per side, submitted shares = traded + waiting; dupIds = repeated trade ids;");
        System.out.println("crashes = orders that made the unsynchronized list throw.");
        System.out.println("'balanced' can stay yes while 'overfilled' is large: a negative waiting amount cancels a");
        System.out.println("double sale. It turns to NO when updates are lost, which is likelier on many-core machines.");
        System.out.println("Races depend on timing: numbers differ from run to run. Run it again.");
    }

    // ---- the two books -----------------------------------------------------------------------------------------

    private static Book racyBook() {
        RacyOrderBook racy = new RacyOrderBook();
        return new Book() {
            public List<Transaction> process(Order order) { return racy.process(order); }
            public int waiting(OrderType side) { return racy.openQuantity(side); }
            public int overfilled() { return racy.overfilledOrders().size(); }
        };
    }

    private static Book lockedBook() {
        OrderBook locked = new OrderBook("AAPL", 10_000);
        return new Book() {
            public List<Transaction> process(Order order) { return locked.process(order); }
            public int waiting(OrderType side) {
                MarketSnapshot snapshot = locked.getSnapshot();
                return side == OrderType.BUY ? snapshot.openBuyShares() : snapshot.openSellShares();
            }
            // OrderBook removes a filled order inside the lock, so a waiting order always has shares left.
            public int overfilled() { return 0; }
        };
    }

    // ---- one round ---------------------------------------------------------------------------------------------

    private record Result(int overfilled, boolean balanced, long duplicateIds, int crashes) {
        boolean corrupted() {
            return overfilled > 0 || !balanced || duplicateIds > 0 || crashes > 0;
        }
    }

    private static Result run(Supplier<Book> newBook) throws InterruptedException, ExecutionException {
        Book book = newBook.get();
        Queue<Transaction> history = new ConcurrentLinkedQueue<>();
        AtomicInteger crashes = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(BUYERS + SELLERS);
        List<Future<Integer>> buyerResults = new ArrayList<>();
        List<Future<Integer>> sellerResults = new ArrayList<>();
        for (int i = 0; i < BUYERS; i++) {
            buyerResults.add(pool.submit(trader(OrderType.BUY, book, history, crashes)));
        }
        for (int i = 0; i < SELLERS; i++) {
            sellerResults.add(pool.submit(trader(OrderType.SELL, book, history, crashes)));
        }
        pool.shutdown();

        int sharesToBuy = total(buyerResults);
        int sharesToSell = total(sellerResults);
        int traded = history.stream().mapToInt(Transaction::quantity).sum();
        boolean balanced = sharesToBuy == traded + book.waiting(OrderType.BUY)
                && sharesToSell == traded + book.waiting(OrderType.SELL);
        long duplicateIds = history.size() - history.stream().map(Transaction::id).distinct().count();
        return new Result(book.overfilled(), balanced, duplicateIds, crashes.get());
    }

    private static void print(int round, Result result) {
        System.out.printf("  round %d: overfilled=%-4d balanced=%-3s dupIds=%-3d crashes=%-2d -> %s%n",
                round, result.overfilled(), result.balanced() ? "yes" : "NO", result.duplicateIds(), result.crashes(),
                result.corrupted() ? "CORRUPTED" : "ok");
    }

    /** A trader that sends orders at exactly $100.00, one side only. Returns the shares it submitted. */
    private static Callable<Integer> trader(OrderType side, Book book, Queue<Transaction> history,
                                            AtomicInteger crashes) {
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int sharesSubmitted = 0;
            for (int i = 0; i < ORDERS_PER_TRADER; i++) {
                int quantity = 1 + random.nextInt(5);
                Order order = new Order(side + "-trader", "AAPL", side, 10_000, quantity);
                try {
                    history.addAll(book.process(order));
                } catch (RuntimeException corruptedList) {      // an unsynchronized ArrayList can even throw
                    crashes.incrementAndGet();
                }
                sharesSubmitted += quantity;
            }
            return sharesSubmitted;
        };
    }

    private static int total(List<Future<Integer>> results) throws InterruptedException, ExecutionException {
        int total = 0;
        for (Future<Integer> result : results) {
            total += result.get();
        }
        return total;
    }
}
