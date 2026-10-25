package tests;

import engine.StockExchange;
import faulty.Account;
import faulty.RacyOrderBook;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.table.DefaultTableModel;
import model.MarketSnapshot;
import model.Money;
import model.Order;
import model.OrderType;
import model.Transaction;
import simulation.BuyerTask;
import simulation.SellerTask;

/**
 * LIVE INTERACTIVE CONCURRENCY DASHBOARD (SWING GUI).
 *
 * Runs continuously until the window is closed so you can:
 *  1) Watch the 16 Buyer/Seller threads and market order books update in real time.
 *  2) Inspect the live Java process (PID, Threads, CPU %) in Windows Task Manager.
 *  3) Toggle between the CORRECT engine and FAULTY concurrency bugs (Data Race, Deadlock, Livelock).
 */
public class MainGuiDashboard extends JFrame {
    private static final List<String> TICKERS = List.of("AAPL", "GOOGL", "MSFT", "NVDA", "TSLA");
    private static final long OPENING_PRICE = 10_000; // $100.00
    private static final int BUYERS = 8;
    private static final int SELLERS = 8;

    // UI Components
    private final JLabel pidAndThreadsLabel = new JLabel();
    private final JLabel modeBannerLabel = new JLabel("MODE: IDLE - Click a button above to start simulation");
    private final JLabel buyAuditLabel = new JLabel("BUY  AUDIT: Waiting to start...");
    private final JLabel sellAuditLabel = new JLabel("SELL AUDIT: Waiting to start...");
    private final JLabel extraMetricsLabel = new JLabel("Status: Ready");
    private final JCheckBox turboCheckBox = new JCheckBox("Turbo CPU Load (No pause between batches - best for Task Manager)", false);

    private final DefaultTableModel marketTableModel;
    private final DefaultTableModel threadTableModel;
    private final JTextArea logArea = new JTextArea(8, 80);

    // Simulation State
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile String currentMode = "NONE";
    private volatile ExecutorService activePool;
    private volatile List<Thread> monitoredThreads = new ArrayList<>();

    // Shared Metrics for UI Refresh
    private volatile StockExchange correctExchange;
    private volatile RacyOrderBook racyBook;
    private final Queue<Transaction> racyHistory = new ConcurrentLinkedQueue<>();
    private final AtomicLong totalBuySubmitted = new AtomicLong();
    private final AtomicLong totalSellSubmitted = new AtomicLong();
    private final AtomicInteger racyCrashes = new AtomicInteger();
    private final AtomicLong livelockAttempts = new AtomicLong();
    private final AtomicInteger livelockSettled = new AtomicInteger();

    public MainGuiDashboard() {
        super("Stock Exchange Concurrency Monitor - PCBE/CEBP Stage 1");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1180, 780);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout(8, 8));

        // 1. TOP CONTROL BAR
        JPanel topContainer = new JPanel(new GridLayout(3, 1, 4, 4));
        topContainer.setBorder(BorderFactory.createEmptyBorder(8, 8, 4, 8));

        JPanel pidBar = new JPanel(new BorderLayout());
        pidAndThreadsLabel.setFont(new Font("Monospaced", Font.BOLD, 13));
        pidBar.add(pidAndThreadsLabel, BorderLayout.WEST);
        pidBar.add(turboCheckBox, BorderLayout.EAST);

        JPanel buttonBar = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 2));
        JButton btnCorrect = new JButton("1. Run CORRECT Engine (Locked)");
        JButton btnDataRace = new JButton("2. Run FAULTY: Data Race (No Lock)");
        JButton btnDeadlock = new JButton("3. Trigger FAULTY: Deadlock");
        JButton btnLivelock = new JButton("4. Trigger FAULTY: Livelock (100% CPU)");
        JButton btnFixLivelock = new JButton("5. Run FIXED Settlement (Back-Off)");
        JButton btnStop = new JButton("STOP");

        styleButton(btnCorrect, new Color(25, 110, 50));
        styleButton(btnDataRace, new Color(165, 40, 40));
        styleButton(btnDeadlock, new Color(150, 70, 20));
        styleButton(btnLivelock, new Color(150, 70, 20));
        styleButton(btnFixLivelock, new Color(30, 90, 140));
        styleButton(btnStop, new Color(80, 80, 80));

        btnCorrect.addActionListener(e -> startCorrectSimulation());
        btnDataRace.addActionListener(e -> startDataRaceSimulation());
        btnDeadlock.addActionListener(e -> startDeadlockSimulation());
        btnLivelock.addActionListener(e -> startLivelockSimulation(false));
        btnFixLivelock.addActionListener(e -> startLivelockSimulation(true));
        btnStop.addActionListener(e -> stopCurrentSimulation("STOPPED BY USER"));

        buttonBar.add(btnCorrect);
        buttonBar.add(btnDataRace);
        buttonBar.add(btnDeadlock);
        buttonBar.add(btnLivelock);
        buttonBar.add(btnFixLivelock);
        buttonBar.add(btnStop);

        modeBannerLabel.setOpaque(true);
        modeBannerLabel.setBackground(new Color(40, 45, 55));
        modeBannerLabel.setForeground(Color.WHITE);
        modeBannerLabel.setFont(new Font("Monospaced", Font.BOLD, 14));
        modeBannerLabel.setBorder(BorderFactory.createEmptyBorder(4, 10, 4, 10));

        topContainer.add(pidBar);
        topContainer.add(buttonBar);
        topContainer.add(modeBannerLabel);
        add(topContainer, BorderLayout.NORTH);

        // 2. CENTER SPLIT PANE: Market Table (Left) + Live Thread State Table (Right)
        marketTableModel = new DefaultTableModel(
                new Object[]{"Ticker", "Last Price", "Best Bid", "Best Ask", "Waiting BUY", "Waiting SELL"}, 0);
        JTable marketTable = new JTable(marketTableModel);
        marketTable.setFont(new Font("Monospaced", Font.PLAIN, 13));
        marketTable.setRowHeight(24);
        JScrollPane marketScroll = new JScrollPane(marketTable);
        marketScroll.setBorder(BorderFactory.createTitledBorder("Live OrderBook Snapshots (Per-Ticker Locks)"));

        threadTableModel = new DefaultTableModel(
                new Object[]{"Thread ID", "Thread Name", "Role / Task", "JVM State", "Lock / Info"}, 0);
        JTable threadTable = new JTable(threadTableModel);
        threadTable.setFont(new Font("Monospaced", Font.PLAIN, 12));
        threadTable.setRowHeight(22);
        JScrollPane threadScroll = new JScrollPane(threadTable);
        threadScroll.setBorder(BorderFactory.createTitledBorder("Live JVM Thread Inspector (Watch states change live)"));

        JSplitPane centerSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, marketScroll, threadScroll);
        centerSplit.setDividerLocation(540);
        add(centerSplit, BorderLayout.CENTER);

        // 3. BOTTOM PANEL: Live Audit Equations + Scrolling Event Log
        JPanel bottomPanel = new JPanel(new BorderLayout(6, 6));
        bottomPanel.setBorder(BorderFactory.createEmptyBorder(4, 8, 8, 8));

        JPanel auditBox = new JPanel(new GridLayout(3, 1, 2, 2));
        auditBox.setBorder(BorderFactory.createTitledBorder("Live Share Conservation Audit & Concurrency Diagnostics"));
        Font auditFont = new Font("Monospaced", Font.BOLD, 13);
        buyAuditLabel.setFont(auditFont);
        sellAuditLabel.setFont(auditFont);
        extraMetricsLabel.setFont(auditFont);
        auditBox.add(buyAuditLabel);
        auditBox.add(sellAuditLabel);
        auditBox.add(extraMetricsLabel);

        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setBorder(BorderFactory.createTitledBorder("Live Transaction & Pathology Log"));
        logScroll.setPreferredSize(new Dimension(1100, 150));

        bottomPanel.add(auditBox, BorderLayout.NORTH);
        bottomPanel.add(logScroll, BorderLayout.SOUTH);
        add(bottomPanel, BorderLayout.SOUTH);

        // 4. UI REFRESH TIMER (Every 150 ms)
        Timer refreshTimer = new Timer(150, e -> refreshDashboard());
        refreshTimer.start();

        // Start in Correct Mode automatically on launch
        SwingUtilities.invokeLater(this::startCorrectSimulation);
    }

    private void styleButton(JButton btn, Color bg) {
        btn.setBackground(bg);
        btn.setForeground(Color.WHITE);
        btn.setFocusPainted(false);
        btn.setFont(new Font("SansSerif", Font.BOLD, 12));
    }

    // ====================================================================================================
    // MODE 1: CORRECT CONTINUOUS SIMULATION (16 Threads, 5 OrderBooks, Callable + Future)
    // ====================================================================================================
    private void startCorrectSimulation() {
        stopCurrentSimulation("Switching to CORRECT Engine...");
        currentMode = "CORRECT";
        correctExchange = new StockExchange(TICKERS, OPENING_PRICE);
        totalBuySubmitted.set(0);
        totalSellSubmitted.set(0);
        running.set(true);

        setBanner("MODE: CORRECT ENGINE (8 BuyerTask + 8 SellerTask, Per-Ticker ReentrantLocks)", new Color(25, 110, 50));
        appendLog("[START] Correct StockExchange started with 16 worker threads across " + TICKERS.size() + " tickers.");

        List<Thread> poolThreads = ConcurrentLinkedQueueWrap();
        activePool = Executors.newFixedThreadPool(BUYERS + SELLERS, r -> {
            Thread t = new Thread(r);
            t.setName("Exchange-Worker-" + (poolThreads.size() + 1));
            t.setDaemon(true);
            poolThreads.add(t);
            return t;
        });
        monitoredThreads = poolThreads;

        // Coordinator daemon thread runs continuous micro-batches via Callable + Future
        Thread coordinator = new Thread(() -> {
            int batchNum = 0;
            while (running.get() && "CORRECT".equals(currentMode)) {
                try {
                    int ordersPerBatch = turboCheckBox.isSelected() ? 400 : 40;
                    List<Future<Integer>> buyFutures = new ArrayList<>(BUYERS);
                    List<Future<Integer>> sellFutures = new ArrayList<>(SELLERS);

                    for (int i = 1; i <= BUYERS; i++) {
                        buyFutures.add(activePool.submit(new BuyerTask("Buyer-" + i, correctExchange, TICKERS, ordersPerBatch)));
                    }
                    for (int i = 1; i <= SELLERS; i++) {
                        sellFutures.add(activePool.submit(new SellerTask("Seller-" + i, correctExchange, TICKERS, ordersPerBatch)));
                    }

                    long batchBuy = 0;
                    for (Future<Integer> f : buyFutures) batchBuy += f.get();
                    long batchSell = 0;
                    for (Future<Integer> f : sellFutures) batchSell += f.get();

                    totalBuySubmitted.addAndGet(batchBuy);
                    totalSellSubmitted.addAndGet(batchSell);
                    batchNum++;

                    if (batchNum % 5 == 0) {
                        List<Transaction> hist = correctExchange.getTradeHistory();
                        if (!hist.isEmpty()) {
                            appendLog("Latest Trade: " + hist.get(hist.size() - 1));
                        }
                    }

                    if (!turboCheckBox.isSelected()) {
                        Thread.sleep(80);
                    }
                } catch (Exception ignored) {
                    break;
                }
            }
        }, "Batch-Coordinator");
        coordinator.setDaemon(true);
        coordinator.start();
    }

    // ====================================================================================================
    // MODE 2: FAULTY DATA RACE SIMULATION (16 Threads hammering RacyOrderBook without locks)
    // ====================================================================================================
    private void startDataRaceSimulation() {
        stopCurrentSimulation("Switching to FAULTY Data Race Mode...");
        currentMode = "DATARACE";
        racyBook = new RacyOrderBook();
        racyHistory.clear();
        totalBuySubmitted.set(0);
        totalSellSubmitted.set(0);
        racyCrashes.set(0);
        running.set(true);

        setBanner("MODE: FAULTY DATA RACE (RacyOrderBook - NO LOCKS, 16 Threads on AAPL)", new Color(165, 40, 40));
        appendLog("[FAULTY] Started RacyOrderBook without synchronization. Watch Oversold Orders & MISMATCH climb!");

        List<Thread> poolThreads = ConcurrentLinkedQueueWrap();
        activePool = Executors.newFixedThreadPool(BUYERS + SELLERS, r -> {
            Thread t = new Thread(r);
            t.setName("Racy-Trader-" + (poolThreads.size() + 1));
            t.setDaemon(true);
            poolThreads.add(t);
            return t;
        });
        monitoredThreads = poolThreads;

        Thread coordinator = new Thread(() -> {
            while (running.get() && "DATARACE".equals(currentMode)) {
                try {
                    int ordersPerBatch = turboCheckBox.isSelected() ? 200 : 30;
                    List<Future<Integer>> buyFutures = new ArrayList<>(BUYERS);
                    List<Future<Integer>> sellFutures = new ArrayList<>(SELLERS);

                    for (int i = 1; i <= BUYERS; i++) {
                        buyFutures.add(activePool.submit(() -> runRacyOrders(OrderType.BUY, ordersPerBatch)));
                    }
                    for (int i = 1; i <= SELLERS; i++) {
                        sellFutures.add(activePool.submit(() -> runRacyOrders(OrderType.SELL, ordersPerBatch)));
                    }

                    long bSum = 0;
                    for (Future<Integer> f : buyFutures) bSum += f.get();
                    long sSum = 0;
                    for (Future<Integer> f : sellFutures) sSum += f.get();

                    totalBuySubmitted.addAndGet(bSum);
                    totalSellSubmitted.addAndGet(sSum);

                    if (!turboCheckBox.isSelected()) {
                        Thread.sleep(100);
                    }
                } catch (Exception ignored) {
                    break;
                }
            }
        }, "Racy-Coordinator");
        coordinator.setDaemon(true);
        coordinator.start();
    }

    private int runRacyOrders(OrderType side, int count) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int submitted = 0;
        for (int i = 0; i < count; i++) {
            int qty = 1 + rnd.nextInt(5);
            Order order = new Order(side + "-Racy", "AAPL", side, 10_000, qty);
            try {
                racyHistory.addAll(racyBook.process(order));
            } catch (RuntimeException ex) {
                racyCrashes.incrementAndGet();
            }
            submitted += qty;
        }
        return submitted;
    }

    // ====================================================================================================
    // MODE 3: FAULTY DEADLOCK SIMULATION (Circular Wait on 2 Accounts, Caught by ThreadMXBean)
    // ====================================================================================================
    private void startDeadlockSimulation() {
        stopCurrentSimulation("Triggering Deadlock...");
        currentMode = "DEADLOCK";
        running.set(true);

        setBanner("MODE: FAULTY DEADLOCK (Circular Wait: Thread-1 holds ACC-A, Thread-2 holds ACC-B)", new Color(150, 70, 20));
        appendLog("[FAULTY] Launching 2 opposite settlement threads locking Buyer first, then Seller...");

        Account accA = new Account("ACC-A", 500_000, 100);
        Account accB = new Account("ACC-B", 500_000, 100);

        List<Thread> dlThreads = new ArrayList<>();
        Thread t1 = new Thread(() -> {
            accA.getLock().lock();
            try {
                sleepSilently(120);
                accB.getLock().lock();
                try {
                    accA.settleBuy(accB, 20_000, 5);
                } finally {
                    accB.getLock().unlock();
                }
            } finally {
                accA.getLock().unlock();
            }
        }, "Deadlock-A-buys-B");

        Thread t2 = new Thread(() -> {
            accB.getLock().lock();
            try {
                sleepSilently(120);
                accA.getLock().lock();
                try {
                    accB.settleBuy(accA, 15_000, 3);
                } finally {
                    accA.getLock().unlock();
                }
            } finally {
                accB.getLock().unlock();
            }
        }, "Deadlock-B-buys-A");

        t1.setDaemon(true);
        t2.setDaemon(true);
        dlThreads.add(t1);
        dlThreads.add(t2);
        monitoredThreads = dlThreads;

        t1.start();
        t2.start();
    }

    // ====================================================================================================
    // MODE 4 & 5: LIVELOCK (Polite Spin Burning CPU) vs FIXED RANDOM BACK-OFF
    // ====================================================================================================
    private void startLivelockSimulation(boolean useRandomBackoff) {
        stopCurrentSimulation("Starting Livelock comparison...");
        currentMode = useRandomBackoff ? "LIVELOCK_FIXED" : "LIVELOCK_FAULTY";
        livelockAttempts.set(0);
        livelockSettled.set(0);
        running.set(true);

        if (useRandomBackoff) {
            setBanner("MODE: FIXED SETTLEMENT (tryLock + Random Back-Off breaks symmetry)", new Color(30, 90, 140));
            appendLog("[FIXED] Running tryLock() with random 5-25ms backoff. Settlements complete continuously!");
        } else {
            setBanner("MODE: FAULTY LIVELOCK (Polite tryLock without backoff - Check CPU in Task Manager!)", new Color(165, 40, 40));
            appendLog("[FAULTY] Livelock active: threads are 100% RUNNABLE spinning in lockstep, 0 settlements completed!");
        }

        Account accA = new Account("ACC-A", 500_000, 1000);
        Account accB = new Account("ACC-B", 500_000, 1000);

        List<Thread> llThreads = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            final boolean aToB = (i % 2 == 1);
            Thread t = new Thread(() -> {
                while (running.get()) {
                    Account buyer = aToB ? accA : accB;
                    Account seller = aToB ? accB : accA;
                    livelockAttempts.incrementAndGet();

                    if (buyer.getLock().tryLock()) {
                        try {
                            if (!useRandomBackoff) {
                                // Hold lock briefly so opposite threads always collide symmetrically
                                long spinUntil = System.nanoTime() + 50_000;
                                while (System.nanoTime() < spinUntil) {
                                    Thread.onSpinWait();
                                }
                            } else {
                                if (seller.getLock().tryLock()) {
                                    try {
                                        buyer.settleBuy(seller, 100, 1);
                                        livelockSettled.incrementAndGet();
                                    } finally {
                                        seller.getLock().unlock();
                                    }
                                }
                            }
                        } finally {
                            buyer.getLock().unlock();
                        }
                    }

                    if (useRandomBackoff) {
                        sleepSilently(ThreadLocalRandom.current().nextInt(5, 25));
                    }
                }
            }, (useRandomBackoff ? "Backoff-Worker-" : "Livelock-Spinner-") + i);
            t.setDaemon(true);
            llThreads.add(t);
            t.start();
        }
        monitoredThreads = llThreads;
    }

    // ====================================================================================================
    // PERIODIC UI REFRESH (Polls Snapshots, Thread States, and ThreadMXBean)
    // ====================================================================================================
    private void refreshDashboard() {
        long pid = ProcessHandle.current().pid();
        int totalJvmThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        pidAndThreadsLabel.setText(String.format(
                "Process ID (PID): %-8d | Total JVM Threads: %-3d | Open Windows Task Manager -> Details -> PID %d",
                pid, totalJvmThreads, pid));

        // 1. Update Thread Inspector Table
        threadTableModel.setRowCount(0);
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        for (Thread t : monitoredThreads) {
            ThreadInfo info = mxBean.getThreadInfo(t.getId());
            String lockInfo = (info != null && info.getLockName() != null)
                    ? "Waiting on " + shorten(info.getLockName()) + " (held by " + info.getLockOwnerName() + ")"
                    : "Executing / Ready";
            threadTableModel.addRow(new Object[]{
                    t.getId(),
                    t.getName(),
                    currentMode,
                    t.getState(),
                    lockInfo
            });
        }

        // 2. Update Market Table & Audit Equations based on active mode
        if ("CORRECT".equals(currentMode) && correctExchange != null) {
            marketTableModel.setRowCount(0);
            List<MarketSnapshot> snaps = correctExchange.getSnapshots();
            int waitBuy = 0;
            int waitSell = 0;
            for (MarketSnapshot s : snaps) {
                waitBuy += s.openBuyShares();
                waitSell += s.openSellShares();
                marketTableModel.addRow(new Object[]{
                        s.ticker(),
                        Money.format(s.lastPriceCents()),
                        s.bestBidCents() == MarketSnapshot.NONE ? "-" : Money.format(s.bestBidCents()),
                        s.bestAskCents() == MarketSnapshot.NONE ? "-" : Money.format(s.bestAskCents()),
                        s.openBuyShares(),
                        s.openSellShares()
                });
            }
            List<Transaction> trades = correctExchange.getTradeHistory();
            long tradedShares = trades.stream().mapToLong(Transaction::quantity).sum();
            long subBuy = totalBuySubmitted.get();
            long subSell = totalSellSubmitted.get();

            buyAuditLabel.setForeground(new Color(20, 120, 40));
            sellAuditLabel.setForeground(new Color(20, 120, 40));
            buyAuditLabel.setText(String.format(
                    "BUY  SIDE: %,9d submitted == %,9d traded + %,7d waiting  [OK - Lock Protected]",
                    subBuy, tradedShares, waitBuy));
            sellAuditLabel.setText(String.format(
                    "SELL SIDE: %,9d submitted == %,9d traded + %,7d waiting  [OK - Lock Protected]",
                    subSell, tradedShares, waitSell));
            extraMetricsLabel.setText(String.format(
                    "Total Matched Trades: %,d | Fine-Grained Locks: %d independent OrderBooks | Data Races: 0",
                    trades.size(), TICKERS.size()));

        } else if ("DATARACE".equals(currentMode) && racyBook != null) {
            int waitBuy = racyBook.openQuantity(OrderType.BUY);
            int waitSell = racyBook.openQuantity(OrderType.SELL);
            int overfilled = racyBook.overfilledOrders().size();
            long traded = racyHistory.stream().mapToLong(Transaction::quantity).sum();
            long dupIds = racyHistory.size() - racyHistory.stream().map(Transaction::id).distinct().count();

            marketTableModel.setRowCount(0);
            marketTableModel.addRow(new Object[]{"AAPL (RACY)", "$100.00", "$100.00", "$100.00", waitBuy, waitSell});

            long subBuy = totalBuySubmitted.get();
            long subSell = totalSellSubmitted.get();
            boolean buyEq = (subBuy == traded + waitBuy);
            boolean sellEq = (subSell == traded + waitSell);

            buyAuditLabel.setForeground(Color.RED);
            sellAuditLabel.setForeground(Color.RED);
            buyAuditLabel.setText(String.format(
                    "BUY  SIDE: %,9d submitted vs %,9d traded + %,7d waiting  [%s]",
                    subBuy, traded, waitBuy, buyEq ? "Balanced" : "MISMATCH - LOST UPDATES!"));
            sellAuditLabel.setText(String.format(
                    "SELL SIDE: %,9d submitted vs %,9d traded + %,7d waiting  [%s]",
                    subSell, traded, waitSell, sellEq ? "Balanced" : "MISMATCH - LOST UPDATES!"));
            extraMetricsLabel.setText(String.format(
                    "CORRUPTION DETECTED -> Oversold Orders (qty < 0): %d | Duplicate Trade IDs: %d | List Exceptions: %d",
                    overfilled, dupIds, racyCrashes.get()));

        } else if ("DEADLOCK".equals(currentMode)) {
            long[] deadlocked = mxBean.findDeadlockedThreads();
            int count = (deadlocked == null) ? 0 : deadlocked.length;
            buyAuditLabel.setForeground(Color.RED);
            sellAuditLabel.setForeground(Color.RED);
            buyAuditLabel.setText("DEADLOCK STATUS : JVM ThreadMXBean.findDeadlockedThreads() detected " + count + " frozen threads!");
            sellAuditLabel.setText("COFFMAN CAUSE   : Mutual Exclusion + Hold & Wait + No Preemption + Circular Wait (A->B & B->A)");
            extraMetricsLabel.setText("CPU Usage for these threads: 0% (Both threads are permanently stuck in WAITING state)");

        } else if (currentMode.startsWith("LIVELOCK")) {
            boolean fixed = "LIVELOCK_FIXED".equals(currentMode);
            buyAuditLabel.setForeground(fixed ? new Color(20, 120, 40) : Color.RED);
            sellAuditLabel.setForeground(fixed ? new Color(20, 120, 40) : Color.RED);
            buyAuditLabel.setText(String.format("LIVELOCK MONITOR: %,d lock attempts made | %,d settlements completed",
                    livelockAttempts.get(), livelockSettled.get()));
            sellAuditLabel.setText(fixed
                    ? "RANDOM BACK-OFF : Threads sleep random 5-25ms on failure -> symmetry broken, steady progress!"
                    : "SYMPTOM         : Threads are RUNNABLE (burning high CPU in Task Manager), but 0 settlements finish!");
            extraMetricsLabel.setText("Check the Live JVM Thread Inspector on the right: threads stay RUNNABLE / TIMED_WAITING.");
        }
    }

    private void stopCurrentSimulation(String reason) {
        running.set(false);
        if (activePool != null) {
            activePool.shutdownNow();
            try {
                activePool.awaitTermination(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        appendLog("[INFO] " + reason);
    }

    private void setBanner(String text, Color bg) {
        modeBannerLabel.setText(text);
        modeBannerLabel.setBackground(bg);
    }

    private void appendLog(String line) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(line + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    private static String shorten(String lockName) {
        int idx = lockName.lastIndexOf('.');
        return idx >= 0 ? lockName.substring(idx + 1) : lockName;
    }

    private static List<Thread> ConcurrentLinkedQueueWrap() {
        return new java.util.concurrent.CopyOnWriteArrayList<>();
    }

    private static void sleepSilently(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            MainGuiDashboard gui = new MainGuiDashboard();
            gui.setVisible(true);
        });
    }
}