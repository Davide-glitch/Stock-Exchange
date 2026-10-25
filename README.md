# Stock Exchange  

## Build and run

### Compile (Windows PowerShell)
```powershell
Remove-Item -Recurse -Force target -ErrorAction SilentlyContinue
javac -d target (Get-ChildItem -Recurse -Filter *.java src\main\java).FullName
```

### Compile (Linux / WSL / macOS)
```bash
rm -rf target && mkdir -p target
javac -d target $(find src/main/java -name "*.java")
```

### 1. Core Simulation & Interactive Test Suite
```powershell
java -cp target Main                        # baseline simulation: 16 threads (8 Buyers, 8 Sellers), 3 tickers + audit
java -cp target tests.MainTestSuite         # unified suite: runs all 6 tests (WITH BUG vs WITHOUT BUG) + PASS/FAIL scorecard
java -cp target tests.MainStepVisualizer    # chronological trace: step-by-step timeline showing exact line each bug occurs
java -cp target tests.MainHeavyLoad         # stress benchmark: 32 threads, 10 tickers, 160,000 orders + live volatile ticker tape
java -cp target tests.MainGuiDashboard      # live Swing GUI: real-time order books, PID/thread state inspector & bug toggles
```

### 2. Individual Concurrency Pathology Demos
```powershell
java -cp target faulty.MainDataRace         # faulty 1:  data race / consistency (check-then-act, lost updates, 3 rounds vs locked)
java -cp target faulty.MainCoarseLock       # faulty 2a: performance (global mutex vs per-ticker lock + critical section length)
java -cp target faulty.MainDeadlock         # faulty 2b: deadlock (circular wait caught via ThreadMXBean) + 2 fixes (ID order & tryLock)
java -cp target faulty.MainLivelock         # faulty 2c: livelock (symmetric polite tryLock loop) + fix (random back-off)
java -cp target faulty.MainStarvation       # faulty 2d: starvation (VIP PriorityBlockingQueue) + fix (FIFO LinkedBlockingQueue)
```

## Runtime architecture

### 1. Compact Overview

```text
Main
 '-- ExecutorService (fixed pool, 16 threads)
      |-- 8 x BuyerTask  (Callable<Integer>) --.
      '-- 8 x SellerTask (Callable<Integer>) --+--> StockExchange.submitOrder()
                                                     |
                    .--------------------------------' (lock-free routing)
                    |
                    |--> OrderBook AAPL  [own ReentrantLock] --.
                    |--> OrderBook GOOGL [own ReentrantLock] --+--> unlock()
                    '--> OrderBook MSFT  [own ReentrantLock] --'       |
                                                                       v
                                                        history.addAll(trades)
                                                       (ConcurrentLinkedQueue)
```

### 2. Detailed Data & Lock Flow

```text
                    +---------------------------------------+
                    |              Main Thread              |
                    +-------------------+-------------------+
                                        |
                     submits 16 tasks   |   collects share counts
                                        v   via Future<Integer>.get()
                    +-------------------+-------------------+
                    |  ExecutorService (Fixed Pool: 16)     |
                    |  +-- 8 x BuyerTask  (Callable, BUY)   |
                    |  '-- 8 x SellerTask (Callable, SELL)  |
                    +-------------------+-------------------+
                                        |
                                        | submitOrder(order) / cancelOrder(order)
                                        v
                    +-------------------+-------------------+
                    |      StockExchange (No Locks)         |
                    |   Routes via immutable Map.copyOf()   |
                    +-----+-------------+-------------+-----+
                          |             |             |
           +--------------+             |             +--------------+
           |                            |                            |
           v                            v                            v
+----------+----------+      +----------+----------+      +----------+----------+
|   OrderBook: AAPL   |      |  OrderBook: GOOGL   |      |   OrderBook: MSFT   |
|  (Own ReentrantLock |      |  (Own ReentrantLock |      |  (Own ReentrantLock |
|   + PriorityQueues) |      |   + PriorityQueues) |      |   + PriorityQueues) |
+----------+----------+      +----------+----------+      +----------+----------+
           |                            |                            |
           +--------------+             |             +--------------+
                          |             |             |
                          v             v             v
                       ( Lock released BEFORE recording )
                                        |
                                        v
                    +-------------------+-------------------+
                    |    Trade History (Lock-Free Queue)    |
                    |   ConcurrentLinkedQueue<Transaction>  |
                    +---------------------------------------+
```

## Where each concept lives

| Concept                                     | File                                            |
| :------------------------------------------ | :---------------------------------------------- |
| Separate buyer / seller roles               | `simulation/BuyerTask`, `simulation/SellerTask` |
| Executor, fixed pool, `Callable` + `Future` | `Main`, `simulation/TraderTask`                 |
| Fine-grained lock, short critical section   | `engine/OrderBook`                              |
| Lock-free history, immutable map            | `engine/StockExchange`                          |
| `volatile` last price, `AtomicLong` ids     | `engine/OrderBook`, `model/Order`               |
| Modify/cancel vs matching (same lock)       | `OrderBook.cancel`, `simulation/TraderTask`     |
| Audit (per-side correctness check)          | `Main.report`                                   |
| Data race, check-then-act, lost update      | `faulty/RacyOrderBook`, `faulty/MainDataRace`   |
| Coarse lock vs fine lock (measured)         | `faulty/MainCoarseLock`                         |
| Deadlock, Coffman conditions, 2 fixes       | `faulty/MainDeadlock`, `faulty/Account`         |
| Livelock, `CyclicBarrier`, random back-off  | `faulty/MainLivelock`, `faulty/Account`         |
| Starvation, priority vs FIFO queue          | `faulty/MainStarvation`                         |
| Unified 6-test comparison scorecard         | `tests/MainTestSuite`                           |
| Step-by-step chronological bug trace        | `tests/MainStepVisualizer`                      |
| 32-thread / 10-ticker stress benchmark      | `tests/MainHeavyLoad`                           |
| Live Swing GUI & `ThreadMXBean` inspector   | `tests/MainGuiDashboard`                        |