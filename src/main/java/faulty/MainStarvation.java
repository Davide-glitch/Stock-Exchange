package faulty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FAULTY VERSION #2d: STARVATION, and its fix.
 *
 * Starvation: a task is ready to run but never gets the resource it needs, because other tasks are always served first.
 * The system as a whole keeps working (so it is not a deadlock), but not for everybody.
 *
 * Scenario: ONE matching worker processes order requests from a queue. The queue decides who is served next.
 *   Faulty: PriorityBlockingQueue, VIP requests always go first. VIP requests keep arriving faster than the worker can
 *           handle them, so the single STANDARD request (even though it arrived FIRST) is never served.
 *   Fix:    LinkedBlockingQueue = first come, first served. (In real systems you can keep priorities and add "aging":
 *           the longer a request waits, the higher its priority.)
 *
 * Executor link: Executors.newFixedThreadPool(n) is just a ThreadPoolExecutor with n threads and a LinkedBlockingQueue.
 * Here we build the pool ourselves to choose the queue.
 *
 * Another classic cause of starvation: an UNFAIR lock. A thread that unlocks may lock again before the waiting threads
 * wake up. new ReentrantLock(true) serves waiters in arrival order, at the cost of throughput. That effect depends on
 * the number of cores and the OS scheduler, so it is not demonstrated here.
 */
public class MainStarvation {
    private static final long FLOOD_MILLIS = 2_000;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1) FAULTY: priority queue, VIP requests always go first ===");
        run(new PriorityBlockingQueue<>());
        System.out.println("\n=== 2) FIXED: first come, first served (FIFO queue) ===");
        run(new LinkedBlockingQueue<>());
    }

    private static void run(BlockingQueue<Runnable> queue) throws InterruptedException {
        // One worker thread (= the matching engine). The queue we pass in decides who is served next.
        ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        AtomicInteger vipServed = new AtomicInteger();
        AtomicLong standardWaitedMillis = new AtomicLong(-1);       // -1 = not served yet

        // A pool gives its very first task straight to the new thread, skipping the queue. So we start the worker
        // with one VIP request; everything submitted after it really has to wait in the queue.
        worker.execute(new Request(true, vipServed, standardWaitedMillis));
        // The standard request arrives BEFORE the flood ...
        worker.execute(new Request(false, vipServed, standardWaitedMillis));
        // ... then VIP requests flood in: 2 per millisecond, while the worker needs about 1 ms per request.
        long end = System.nanoTime() + FLOOD_MILLIS * 1_000_000;
        while (System.nanoTime() < end) {
            worker.execute(new Request(true, vipServed, standardWaitedMillis));
            worker.execute(new Request(true, vipServed, standardWaitedMillis));
            Thread.sleep(1);
        }

        long waited = standardWaitedMillis.get();
        System.out.printf("After %d ms of VIP traffic: %d VIP requests served. The standard request %s.%n",
                FLOOD_MILLIS, vipServed.get(),
                waited < 0 ? "has NOT been served: STARVATION" : "was served after waiting " + waited + " ms");

        worker.shutdownNow();                                        // drop the backlog and stop the worker
        worker.awaitTermination(2, TimeUnit.SECONDS);
    }

    /** A request waiting for the worker. As a Runnable it can be queued; as a Comparable it can be prioritized. */
    private static class Request implements Runnable, Comparable<Request> {
        private static final AtomicLong ARRIVALS = new AtomicLong();

        private final boolean vip;
        private final AtomicInteger vipServed;
        private final AtomicLong standardWaitedMillis;
        private final long arrivalNumber = ARRIVALS.getAndIncrement();
        private final long createdNanos = System.nanoTime();

        Request(boolean vip, AtomicInteger vipServed, AtomicLong standardWaitedMillis) {
            this.vip = vip;
            this.vipServed = vipServed;
            this.standardWaitedMillis = standardWaitedMillis;
        }

        @Override
        public void run() {
            if (vip) {
                vipServed.incrementAndGet();
            } else {
                standardWaitedMillis.set((System.nanoTime() - createdNanos) / 1_000_000);
            }
            try {
                Thread.sleep(1);                    // "processing" takes about 1 ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** VIP first; among equals, the earlier arrival first. Only the PriorityBlockingQueue uses this. */
        @Override
        public int compareTo(Request other) {
            if (vip != other.vip) {
                return vip ? -1 : 1;
            }
            return Long.compare(arrivalNumber, other.arrivalNumber);
        }
    }
}
