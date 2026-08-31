<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Matching Engine

The **Matching Engine** (`Engine`) acts as the stateful, high-throughput ingress router and orchestrator of the Nexus Trading Exchange (NTE). Positioned at the entry point of the matching core, it manages the lifecycle of asset-specific order books, routes incoming order traffic via symbol sharding, and records microsecond-level telemetry for execution monitoring.

Operating under the Global Financial Markets Group (GFMG) standard, the `Engine` is designed to maximize concurrency across independent symbols while maintaining strict deterministic state consistency within each individual market.

---

## Responsibilities

The `Engine` is tasked with the following core responsibilities:

*   **Symbol Sharding and Routing:** Inbound orders are dynamically routed to isolated, symbol-specific `[[entities/order-book]]` instances, preventing system-wide lock contention.
*   **Concurrent Registry Management:** Managing the global registry of active symbols (`map[string]*OrderBook`) using safe read-write mechanisms to allow concurrent initialization of new markets without blocking ongoing matching operations.
*   **Telemetry & Latency Stamping:** Tracking ingress-to-egress transaction latency at microsecond resolution to enforce execution SLA tracking and optimize performance against [[decisions/configuration-latency-tuning]].
*   **Downstream Orchestration:** Interfacing directly with the egress publisher (`[[concepts/kafka-integration]]`) to distribute executing matches and top-of-book depth snapshots to the [[entities/market-data-gateway]], [[entities/trade-settlement-system]], and [[entities/compliance-surveillance-monitor]].

---

## Dependencies

The `Engine` serves as an orchestration coordinator and relies on several adjacent system structures:

*   **[[entities/order-book]] (Internal Matcher):** The target execution queue that implements the [[concepts/price-time-priority]] and [[concepts/iceberg-order-engine]] processing algorithms.
*   **[[entities/order-model]] (Inbound Contract):** The immutable input data structure representing a participant's trade instruction.
*   **[[entities/trade-execution-model]] (Outbound Event):** The immutable execution reports returned by the book and published to downstream pipelines.
*   **[[concepts/kafka-integration]] (Egress Streamer):** Enforces immediate, zero-batch routing of execution events and market snapshots to Kafka.

```
┌────────────────────────────────────────────────────────┐
│               [[entities/matching-engine]]             │
│  - Acquires Global R-Lock                              │
│  - Measures Ingress Timestamp (t0)                     │
│  - Routes to Symbol Book                               │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│                [[entities/order-book]]                 │
│  - Acquires Local Book Mutex                           │
│  - Runs Price-Time Priority Matching                   │
│  - Processes [[concepts/iceberg-order-engine]] Slices  │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│             Telemetry & Egress Publishing              │
│  - Measures Execution Duration (t1 - t0)               │
│  - Publishes to [[concepts/kafka-integration]]        │
└────────────────────────────────────────────────────────┘
```

---

## Architectural & Design Mechanisms

### Symbol Sharding and Concurrent Registries
To achieve ultra-low latency, the matching engine avoids a single global matching lock. Instead, it employs the [[decisions/split-locking-pattern]]:
1.  The `Engine` maintains a registry of order books: `books map[string]*OrderBook`.
2.  When an order is received, a global `sync.RWMutex` (`e.mu`) is acquired in **Read-Mode** to locate the pointer for the target asset's `OrderBook`.
3.  If the book does not exist, the lock is upgraded to **Write-Mode** to safely initialize the symbol's book before reverting.
4.  Once the book pointer is resolved, the global lock is immediately released. The engine then calls `AddOrder` on the target `OrderBook`, which serializes matches internally under its own local lock scope.

This pattern ensures that a burst of trading activity in high-volume instruments (e.g., `BTC/USD`) never delays execution matching or order processing in other active pairs.

### Telemetric Latency Stamping
To ensure the NTE operates within its designated latency budgets, the `Engine` records microsecond-level telemetric data. This profiling is baked into the main execution path of the router:

```go
start := time.Now()
trades, err := book.AddOrder(order)
duration := time.Since(start)

e.log.WithFields(logrus.Fields{
    "symbol":     order.Symbol,
    "order_id":   order.ID,
    "match_uS":   duration.Microseconds(),
    "trades_qty": len(trades),
}).Debug("Order processed")
```

This telemetry feeds real-time performance tracking dashboards, alerting operators if execution times spike during heavy market volatility.

---

## Core Execution Flow

The standard path for processing an inbound request through the matching engine is as follows:

```go
package matching

import (
	"fmt"
	"sync"
	"time"

	"github.com/Astrophage/order-matching-engine/models"
	"github.com/sirupsen/logrus"
)

type Engine struct {
	books map[string]*OrderBook
	mu    sync.RWMutex
	log   *logrus.Logger
}

func NewEngine(log *logrus.Logger) *Engine {
	return &Engine{
		books: make(map[string]*OrderBook),
		log:   log,
	}
}

// ProcessOrder routes an order to the correct book and returns resulting trades.
func (e *Engine) ProcessOrder(order models.Order) ([]models.TradeExecution, error) {
	// 1. Acquire global registry lock (R-Lock) to locate the book
	e.mu.Lock()
	book, exists := e.books[order.Symbol]
	if !exists {
		book = NewOrderBook(order.Symbol)
		e.books[order.Symbol] = book
		e.log.WithField("symbol", order.Symbol).Info("Created new order book instance")
	}
	e.mu.Unlock()

	// 2. Perform localized matching and record exact duration
	start := time.Now()
	trades, err := book.AddOrder(order)
	duration := time.Since(start)

	// 3. Emit metrics for latency tracking
	e.log.WithFields(logrus.Fields{
		"symbol":     order.Symbol,
		"order_id":   order.ID,
		"match_uS":   duration.Microseconds(),
		"trades_qty": len(trades),
	}).Debug("Order processed by engine core")

	if err != nil {
		return nil, fmt.Errorf("failed to process order %s: %w", order.ID, err)
	}

	return trades, nil
}
```

## System Recovery and Crash Consistency

Since the `Engine` retains its global registry and book maps purely in-memory to meet microsecond latency targets, its state is volatile. Resiliency is achieved using an event sourcing strategy:
*   In the event of a system crash, the matching engine restarts and subscribes to the inbound Kafka log from the beginning of the trading session (or the last valid EOD snapshot boundary).
*   The raw orders are fed back through the `Engine` in chronological order to reconstitute the exact state of the symbol registry and book depths.
*   For a complete overview of this recovery mechanism, see [[concepts/crash-recovery-durability]].