package engine;

import model.Account;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SettlementEngine {
    private final Map<String, Account> accounts = new ConcurrentHashMap<>();

    public void registerAccount(Account acc) {
        accounts.put(acc.getAccountId(), acc);
    }

    // Demonstrates Deadlock: Circular wait condition (Thread 1: A->B, Thread 2: B->A)
    public void settleDeadlock(String partyAId, String partyBId, double cash, int shares) {
        Account accA = accounts.get(partyAId);
        Account accB = accounts.get(partyBId);

        accA.getLock().lock();
        try {
            System.out.printf("[%s] Acquired %s, waiting for %s...%n",
                    Thread.currentThread().getName(), accA.getAccountId(), accB.getAccountId());
            Thread.sleep(60); // Guarantees second thread captures its first lock

            accB.getLock().lock();
            try {
                accA.transferTo(accB, cash, shares);
            } finally {
                accB.getLock().unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            accA.getLock().unlock();
        }
    }

    // Solution to Deadlock: Total ordering / global acquisition hierarchy
    public void settleOrdered(String partyAId, String partyBId, double cash, int shares) {
        Account accA = accounts.get(partyAId);
        Account accB = accounts.get(partyBId);

        // Always lock the lower ID first to prevent cycles
        Account first = accA.getAccountId().compareTo(accB.getAccountId()) < 0 ? accA : accB;
        Account second = first == accA ? accB : accA;

        first.getLock().lock();
        try {
            second.getLock().lock();
            try {
                accA.transferTo(accB, cash, shares);
            } finally {
                second.getLock().unlock();
            }
        } finally {
            first.getLock().unlock();
        }
    }

    // Demonstrates Livelock: Politeness without progress (tryLock without random backoff)
    public boolean settleLivelock(String partyAId, String partyBId, double cash, int shares) {
        Account accA = accounts.get(partyAId);
        Account accB = accounts.get(partyBId);

        while (!Thread.currentThread().isInterrupted()) {
            if (accA.getLock().tryLock()) {
                try {
                    // Simulates work before acquiring the second lock
                    try { Thread.sleep(10); } catch (InterruptedException ignored) {}

                    if (accB.getLock().tryLock()) {
                        try {
                            accA.transferTo(accB, cash, shares);
                            return true; // Successfully transferred
                        } finally {
                            accB.getLock().unlock();
                        }
                    }
                    // Failed to acquire B: polite release of A, but retries synchronously
                } finally {
                    accA.getLock().unlock();
                }
            }
            // Symmetrical execution without random delay keeps both threads colliding
        }
        return false;
    }

    // Solution to Livelock: Randomized backoff breaks the symmetry
    public boolean settleLivelockFixed(String partyAId, String partyBId, double cash, int shares) {
        Account accA = accounts.get(partyAId);
        Account accB = accounts.get(partyBId);

        while (!Thread.currentThread().isInterrupted()) {
            if (accA.getLock().tryLock()) {
                try {
                    if (accB.getLock().tryLock()) {
                        try {
                            accA.transferTo(accB, cash, shares);
                            return true;
                        } finally {
                            accB.getLock().unlock();
                        }
                    }
                } finally {
                    accA.getLock().unlock();
                }
            }
            // Random backoff breaks the deadlock/livelock resonance
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(5, 30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }
}