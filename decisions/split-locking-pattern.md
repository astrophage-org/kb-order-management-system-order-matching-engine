# Split-Locking Pattern for Multi-Asset Concurrency

## Status

**Accepted**

## Context

In high-performance financial transaction systems such as the [[summaries/order-matching-engine]], maintaining low execution latency under heavy thread contention is a primary constraint. A naive concurrency model would utilize a single global lock over the entire registry of trading pairs. However, this approach introduces severe serialization bottlenecks: an order execution on `BTC/USD` would block completely unrelated order books like `ETH/USD` or `SOL/USD`.

To prevent this thread contention, the engine requires a locking mechanism that satisfies the following constraints:
1. **Symbol Isolation:** Transactions executing on independent financial instruments must run concurrently and without interference.
2. **Dynamic Book Initialization:** The registry must support safe, dynamic initialization of new [[entities/order-book]] instances when a symbol is traded for the first time, without introducing race conditions.
3. **Low Overhead on Hot Paths:** The lock acquisition overhead must be kept to a minimum in the continuous matching phase to maintain microsecond-level latency envelopes.

## Decision

We have implemented a **Split-Locking Pattern** that segregates locking concerns into two distinct tiers: a global read-write lock (`sync.RWMutex`) protecting the registry routing table, and individual local mutexes localized within each [[entities/order-book]].

```
                  Inbound [[entities/order-model]]
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │          [[entities/matching-engine]]         │
         │                                              │
         │  Step 1: Acquire Registry Read Lock (RLock)  │
         │  - Locate Symbol in Registry Map             │
         │  Step 2: Release Registry Read Lock          │
         └──────────────────────┬───────────────────────┘
                                │
                                ├───────────────────────────────┐
                                ▼                               ▼
                 ┌─────────────────────────────┐ ┌─────────────────────────────┐
                 │    OrderBook [BTC/USD]      │ │    OrderBook [ETH/USD]      │
                 │                             │ │                             │
                 │  Step 3: Acquire Local Lock │ │  Step 3: Acquire Local Lock │
                 │  - Run [[concepts/price-time-priority]]      │ │  - Run [[concepts/price-time-priority]]      │
                 │  - Process [[concepts/iceberg-order-engine]]  │ │  - Process [[concepts/iceberg-order-engine]]  │
                 │  - Generate [[entities/trade-execution-model]]s │ │  - Generate [[entities/trade-execution-model]]s │
                 │  Step 4: Release Local Lock │ │  Step 4: Release Local Lock │
                 └─────────────────────────────┘ └─────────────────────────────┘
```

### Tier 1: Global Registry Lock

The registry map (`map[string]*OrderBook`) is housed inside the main [[entities/matching-engine]]. Because this registry is mostly read (order-routing lookups occur millions of times per second, whereas new books are only initialized once upon instrument onboarding), a Go `sync.RWMutex` is employed.

* **Read Path (Hot Path):** The engine acquires a read lock (`RLock()`), retrieves the pointer to the requested [[entities/order-book]], and immediately releases the read lock (`RUnlock()`). This ensures that multiple execution workers can concurrently retrieve different book pointers without serialization.
* **Write Path (Cold Path):** If a symbol lookup fails because the book does not yet exist, the engine escalates to a write lock (`Lock()`), double-checks existence to prevent race conditions, instantiates the new [[entities/order-book]], and releases the write lock (`Unlock()`).

### Tier 2: Isolated Book Lock

Once the pointer to the target [[entities/order-book]] is successfully resolved, all subsequent transaction processing is delegated entirely to that isolated book instance. 

* Each book operates on its own internal execution lock.
* The book-level lock serializes orders arriving *for that specific asset* to guarantee the deterministic sequence required by the [[concepts/price-time-priority]] FIFO algorithm.
* This lock protects the integrity of the resting order arrays (bids and asks), the local display queues, and the replenishment states managed by the [[concepts/iceberg-order-engine]].

### Implementation Reference

The following simplified snippet from `internal/matching/engine.go` demonstrates the lock-split workflow:

```go
func (e *Engine) ProcessOrder(order models.Order) ([]models.TradeExecution, error) {
	// Tier 1 Locking: Resolve the book pointer safely.
	// NOTE: In production, optimization utilizes RLock first, upgrading to a 
	// Write Lock only if the symbol map entry is missing.
	e.mu.Lock()
	book, exists := e.books[order.Symbol]
	if !exists {
		book = NewOrderBook(order.Symbol)
		e.books[order.Symbol] = book
		e.log.Infof("Created new order book for symbol: %s", order.Symbol)
	}
	e.mu.Unlock() // Tier 1 lock is released before executing match logic

	// Tier 2 Isolation: Matching occurs within the book's internal scope.
	// This prevents BTC transactions from blocking ETH transactions.
	start := time.Now()
	trades, err := book.AddOrder(order)
	duration := time.Since(start)
    
    // Telemetry and egress operations follow...
}
```

## Consequences

### Consequences Analysis

* **High-Throughput Concurrency:** Real-time throughput scales linearly with the core count of the host machine, as matching operations for different symbols run on fully isolated threads.
* **Low Latency Jitter:** Latency spikes are constrained to individual busy symbols. A massive spike in trade volume for a high-liquidity asset like `BTC/USD` will not cause thread starvation or latency degradation in low-volume altcoin pairs.
* **Simplified Deadlock Avoidance:** Because orders are strictly sharded by symbol and processed within a single book, there are no multi-book transactions. A worker thread never needs to acquire more than one book lock at a time, rendering cross-book deadlocks mathematically impossible in this architecture.
* **Memory Alignment & CPU Cache:** By isolating memory structures to specific symbol books, memory access patterns align nicely with CPU caching strategies, reducing cache-line bouncing across multi-socket systems.
* **Integration Downstream:** The resulting trade records are emitted cleanly to [[concepts/kafka-integration]] with symbol-keyed partitioning, preserving the strict chronological sequence established by the localized book lock.

### Related System Configurations

For maximum hardware efficiency when deploying the split-locking pattern, see the tuning parameters defined in [[decisions/configuration-latency-tuning]], which details thread affinity, Kafka partition isolation, and low-latency client integration strategies. For system initialization and recovery, see [[concepts/crash-recovery-durability]].