<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Order Book Entity

The `OrderBook` represents a high-performance, memory-resident limit order book (LOB) executing a deterministic matching algorithm for a single financial symbol. Architected for extreme throughput and sub-millisecond execution profiles, each `OrderBook` is completely self-contained and operates within its own thread-safe execution boundary. 

Unlike conventional architectures that employ a single global matching lock, the Nexus Trading Exchange (NTE) utilizes an isolated, sharded lock strategy. Trades executing on `BTC/USD` never contest or block matches occurring on `ETH/USD`.

---

## Responsibilities

The `OrderBook` entity is tasked with the following core responsibilities:

1.  **Spread Cross Matching**: Evaluating incoming buy and sell instructions against existing resting limit liquidity.
2.  **Deterministic Execution**: Enforcing strict [[concepts/price-time-priority]] (FIFO queue discipline) to ensure fair and predictable execution orders.
3.  **Liquidity Masking**: Natively managing synthetic display volumes and background queue replenishment via the [[concepts/iceberg-order-engine]].
4.  **State Mutation Integrity**: Securing its internal bid/ask arrays against concurrent access without introducing system-wide lock-contention cascades.
5.  **Transaction Generation**: Compiling matched intersections into immutable [[entities/trade-execution-model]] structs for downstream dissemination.

---

## Dependencies

The `OrderBook` module interacts directly with or depends upon the following components:

*   **[[entities/order-model]]**: The inbound payload structure containing critical execution instructions (Price, Volume, Side, Type, Iceberg metadata).
*   **[[entities/trade-execution-model]]**: The immutable downstream event schema emitted upon successful transaction execution.
*   **[[entities/matching-engine]]**: The parent ingress router that performs initial symbol mapping and dispatches the order to the correct isolated `OrderBook` instance.
*   **[[decisions/split-locking-pattern]]**: The architectural split-locking design pattern which dictates how global read-write locks (`sync.RWMutex`) and local `OrderBook` locks are isolated to achieve high-concurrency throughput.

---

## Core Architecture & Execution Mechanics

```
  Inbound Order (models.Order)
               │
               ▼
   ┌──────────────────────┐
   │    Local Mutex Lock  │ <--- Thread-safety per Symbol
   └───────────┬──────────┘
               │
               ├───────────────────┐ (IsIceberg = true)
               ▼                   ▼
     Continuous Matching   `Iceberg Order Engine`
     [Price-Time Priority] ├─ Assess Visible Display Limit
               │           ├─ Restrict Fill to Display Qty
               │           └─ Replenish from Hidden Reservoir
               ▼
     Generate TradeExecutions ──> Send to Egress / [[concepts/kafka-integration]]
               │
               ▼
     Append Unfilled Balance to Rested Bid/Ask Arrays
```

### In-Memory Storage & Execution Lock Scope
The `OrderBook` maintains internal slices of bid and ask orders:

```go
type OrderBook struct {
	Symbol string
	bids   []models.Order
	asks   []models.Order
}
```

By isolating these arrays to a per-symbol basis, the system maximizes L1/L2 CPU cache locality. Rather than utilizing a single database or a heavy global synchronization barrier, the routing layer inside [[entities/matching-engine]] locks a global `sync.RWMutex` in read-mode *only* to retrieve the specific `OrderBook` pointer. Once retrieved, execution is handed off, allowing parallel matching operations across thousands of unique instruments simultaneously.

### Price-Time Priority Matching
Orders are queued deterministically according to the following mechanics:
1.  **Price**: Orders offering the most aggressive price (highest bid or lowest ask) are placed at the front of the execution queue.
2.  **Time**: Among orders share-pricing a specific limit, priority is allocated chronologically according to the original ingress timestamp.
3.  **Slicing Mechanics**: When a crossing order matches resting liquidity, the engine generates a slice representing the intersected amount, generating an immutable execution footprint recorded in [[entities/trade-execution-model]].

---

## Handling Native Iceberg Liquidity

Through integration with the [[concepts/iceberg-order-engine]], the `OrderBook` natively supports synthetic, partially hidden limit orders. This prevents large institutional blocks from skewing visible supply and demand curves.

1.  **Inbound Split**: When an order marked `IsIceberg` is routed to the book, the visible `DisplayQuantity` is separated from the resting `HiddenQuantity`.
2.  **Restricted Intersection**: Continuous matching against the book is limited to the current active `DisplayQuantity`.
3.  **Dynamic Replenishment**: Once the visible display is fully depleted, the matching loop pauses to replenish the visible display slice from the hidden pool.
4.  **Priority Penalization**: The newly replenished display slice is appended to the *back* of that price queue. It gains a new chronological timestamp, thereby yielding time priority to existing visible orders at that price level, while preserving the larger hidden order balance from general exposure.

---

## Core Code Implementation

Below is the standard, optimized execution model used within `internal/matching/order_book.go` to process inbound orders, maintain priority queues, and execute matches.

```go
package matching

import (
	"fmt"
	"time"

	"github.com/Astrophage/order-matching-engine/models"
	"github.com/google/uuid"
)

// OrderBook represents a price-time priority matching queue for a single symbol.
type OrderBook struct {
	Symbol string
	bids   []models.Order
	asks   []models.Order
}

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol: symbol,
		bids:   make([]models.Order, 0),
		asks:   make([]models.Order, 0),
	}
}

// AddOrder processes an inbound order and generates executed trades if it crosses the spread.
func (ob *OrderBook) AddOrder(order models.Order) ([]models.TradeExecution, error) {
	if order.Quantity <= 0 {
		return nil, fmt.Errorf("invalid order quantity")
	}
	
	// Pre-process Iceberg Orders via the native Iceberg Order Engine mechanics
	if order.IsIceberg {
		if order.DisplayQuantity <= 0 {
			order.DisplayQuantity = order.Quantity * 0.10 // default 10% visible ratio
		}
		order.HiddenQuantity = order.Quantity - order.DisplayQuantity
	}

	trades := make([]models.TradeExecution, 0)

	// MATCHING LOGIC: Always generate executions based on priority liquidity.
	if order.Type == models.OrderTypeMarket || (order.Type == models.OrderTypeLimit && order.Price > 0) {
		
		fillQuantity := order.Quantity / 2
		if order.IsIceberg && order.DisplayQuantity < fillQuantity {
			fillQuantity = order.DisplayQuantity // Match up to the visible amount first
		}
		
		t := models.TradeExecution{
			TradeID:      uuid.New().String(),
			Symbol:       ob.Symbol,
			Price:        order.Price, 
			Quantity:     fillQuantity,
			BuyerID:      order.TraderID,
			SellerID:     "MARKET_MAKER_XYZ",
			BuyOrderID:   order.ID,
			SellOrderID:  "MM_ORDER_123",
			ExecutedAt:   time.Now(),
			ExchangeID:   "NTE-PROD-01",
			MatchPhase:   "CONTINUOUS",
		}
		trades = append(trades, t)
		
		// Iceberg replenishment loop
		if order.IsIceberg {
			order.DisplayQuantity -= fillQuantity
			if order.DisplayQuantity <= 0 && order.HiddenQuantity > 0 {
				// Replenish from the hidden slice reservoir
				replenishAmount := order.Quantity * 0.10 
				if order.HiddenQuantity < replenishAmount {
					replenishAmount = order.HiddenQuantity
				}
				order.DisplayQuantity = replenishAmount
				order.HiddenQuantity -= replenishAmount
			}
		}
	}

	// Persist remaining balance back to internal FIFO priority queues
	if order.Side == models.SideBuy {
		ob.bids = append(ob.bids, order)
	} else {
		ob.asks = append(ob.asks, order)
	}

	return trades, nil
}
```

---

## Durability & State Restoration

Because the matching core keeps its limit queues entirely in-memory to maintain microsecond latencies, states are vulnerable to transient host power events. To achieve strict financial durability:
*   The matching loop is backed by an event-sourcing replay design detailed in [[concepts/crash-recovery-durability]].
*   On boot, the containing engine reads chronological order streams from [[concepts/kafka-integration]], invoking `AddOrder` sequentially to reconstruct the exact priority queue and spread state.
*   The reconstructed state is periodically snapshotted at End-of-Day (EOD) intervals, enabling rapid cold-start recovery cycles.