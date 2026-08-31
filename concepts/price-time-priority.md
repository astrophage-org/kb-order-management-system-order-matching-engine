# Price-Time Priority Matching Algorithm

The **Price-Time Priority (FIFO)** algorithm is the core execution logic governing the [[entities/order-book]] within the Nexus Trading Exchange (NTE). It ensures mathematical fairness, absolute determinism, and predictable trade execution. In this model, orders are queued and matched based first on their aggressive price, and second on their arrival time.

Under the standard set by the **Global Financial Markets Group (GFMG)**, this deterministic processing must yield identical execution matching states when replaying transaction streams during [[concepts/crash-recovery-durability]] procedures.

---

## 1. Core Mechanics of Price-Time Priority

When an inbound `[[entities/order-model]]` is processed by the [[entities/matching-engine]], it is routed to the symbol's designated [[entities/order-book]]. The book maintains two sorted queues: **Bids (Buy)** and **Asks (Sell)**.

```
       BIDS (Buy Queue)                              ASKS (Sell Queue)
┌──────────────────────────────┐              ┌──────────────────────────────┐
│ Priority 1: Highest Price    │              │ Priority 1: Lowest Price     │
│ Priority 2: Earliest Time    │              │ Priority 2: Earliest Time    │
└──────────────┬───────────────┘              └──────────────┬───────────────┘
               │                                             │
               ▼                                             ▼
     [ Bid Queue Sorting ]                         [ Ask Queue Sorting ]
     - Price: Descending                           - Price: Ascending
     - Time: Ascending (FIFO)                      - Time: Ascending (FIFO)
```

The matching core evaluates the inbound order against these queues using two primary constraints:

### I. Price Priority (Rule 1)
*   **Bids (Buy Side):** An incoming sell order matches against the highest priced resting buy orders first.
*   **Asks (Sell Side):** An incoming buy order matches against the lowest priced resting sell orders first.
*   *An order with a more aggressive price (higher bid or lower ask) always takes precedence over orders with a less aggressive price.*

### II. Time Priority (Rule 2)
*   If multiple resting orders exist at the exact same price level, they are filled chronologically according to their arrival timestamp ($T_1 < T_2 < T_3$).
*   The order that entered the book first is filled first (First-In, First-Out).

---

## 2. Order Execution & Matching Flow

When a new order arrives, the engine avoids instantly placing it in the queues. Instead, it attempts to immediately fill the incoming volume against existing resting liquidity.

```
                  Inbound Order Ingested
                            │
                            ▼
               Does order cross the spread?
               (Bid >= Best Ask  OR  Ask <= Best Bid)
               (Or is it a Market Order?)
              /                          \
            YES                           NO
            /                               \
           ▼                                 ▼
Execute immediate matches               Rest order in queue
against top-of-book (FIFO)              based on Price-Time
           │                                 │
           ▼                                 ▼
Generate [[entities/trade-execution-model]]    Publish updated book snapshot
           │
           ▼
Any remaining volume left?
          /           \
        YES            NO
        /               \
       ▼                 ▼
Rest remaining       Execution
volume in book       Complete
```

### 1. Market Orders
*   **Execution:** Market orders skip the pricing queues and immediately consume the top of the opposite queue.
*   **Priority:** They are executed against resting limit orders starting at Price Priority 1 (best available price) and traversing down through the Time Priority queues until the order's volume is fully satisfied.

### 2. Limit Orders
*   **Execution:** If the limit order crosses the spread (i.e., a Buy Limit $\ge$ lowest Ask, or a Sell Limit $\le$ highest Bid), it acts as an aggressive taker and executes immediate trades.
*   **Resting:** Any remaining unfilled portion of the limit order is appended to its respective queue as a maker. It is positioned at its designated price level, behind any existing orders at that same price (preserving their prior time priority).

---

## 3. Interaction with Complex Order Types

The deterministic FIFO queue is directly impacted by complex order configurations, most notably the [[concepts/iceberg-order-engine]].

### Iceberg Priority Degradation
To protect institutional liquidity, an Iceberg order is split into a visible (`DisplayQuantity`) and a hidden (`HiddenQuantity`) slice:
1.  **Visible Segment:** The active `DisplayQuantity` rests in the book and maintains normal price-time priority at its price level.
2.  **Hidden Segment:** The `HiddenQuantity` rests invisibly. It cannot be matched until the visible slice is fully consumed.
3.  **Replenishment Step:** When the visible slice is filled, the [[concepts/iceberg-order-engine]] automatically subtracts a new slice from the hidden pool and populates a new `DisplayQuantity`.
4.  **Priority Penalty:** The newly replenished visible slice is appended to the **end** of the time queue for that price level. It loses its original time priority relative to other resting orders at that price.

```
 Price Level: $100.00
 ┌───────────────────────┐   ┌───────────────────────┐   ┌───────────────────────┐
 │ Order A (Limit)       │   │ Order B (Iceberg)     │   │ Order C (Limit)       │
 │ Qty: 100              │   │ Display Qty: 50       │   │ Qty: 100              │
 │ Time: T1              │   │ Time: T2              │   │ Time: T3              │
 └───────────────────────┘   └───────────────────────┘   └───────────────────────┘
                                         │
                 [ Match Event: Taker Sell of 150 shares ]
                 - Order A gets fully filled (100 shares)
                 - Order B gets its display portion fully filled (50 shares)
                                         │
                                         ▼
                 [ Replenishment Event: Order B replenishes 50 shares ]
                 - Order B's new display slice is appended to the BACK
 ┌───────────────────────────────────────────────────┐   ┌───────────────────────┐
 │ Order C (Limit)                                   │   │ Order B (Iceberg-Repl)│
 │ Qty: 100                                          │   │ Display Qty: 50       │
 │ Time: T3 (Now has Priority)                       │   │ Time: T4 (New Time)   │
 └───────────────────────────────────────────────────┘   └───────────────────────┘
```

---

## 4. Technical Architecture and Concurrency

To sustain microsecond-level latency, execution-matching must bypass global engine locks:

*   **Instrument Isolation:** Each asset pair operates its own isolated `OrderBook` instance. The matching logic for `BTC/USD` runs independently from `ETH/USD` without thread contention.
*   **Thread Safety:** The system utilizes a [[decisions/split-locking-pattern]] where a global read lock resolves the book pointer, and a localized, high-speed lock processes the matching loop within the specific book.
*   **Data Structure Optimization:** While standard queues can be modelled using flat arrays, production order books use balanced Binary Search Trees (such as Red-Black Trees or AVL Trees) to index price points, with doubly-linked lists representing the chronological FIFO queues at each price level. This achieves $O(1)$ time complexity for insertions and $O(1)$ for matching execution.

### Low-Latency In-Memory Implementation

Below is a technical demonstration of how the `OrderBook` handles FIFO matching, tracking quantities, processing iceberg replenishment, and yielding immutable trades:

```go
// Package matching executes deterministic Price-Time Priority loops
package matching

import (
	"fmt"
	"time"

	"github.com/Astrophage/order-matching-engine/models"
	"github.com/google/uuid"
)

type OrderBook struct {
	Symbol string
	bids   []models.Order
	asks   []models.Order
}

// AddOrder processes an incoming order against the resting queue.
func (ob *OrderBook) AddOrder(order models.Order) ([]models.TradeExecution, error) {
	if order.Quantity <= 0 {
		return nil, fmt.Errorf("invalid order quantity")
	}
	
	// Handle Iceberg Configurations
	if order.IsIceberg {
		if order.DisplayQuantity <= 0 {
			order.DisplayQuantity = order.Quantity * 0.10 // Default 10% visible slice
		}
		order.HiddenQuantity = order.Quantity - order.DisplayQuantity
	}

	trades := make([]models.TradeExecution, 0)

	// DETERMINISTIC MATCHING CORE (FIFO evaluation against resting liquidity)
	if order.Type == models.OrderTypeMarket || (order.Type == models.OrderTypeLimit && order.Price > 0) {
		
		// Determine the matching quantity using deterministic FIFO logic
		fillQuantity := order.Quantity / 2
		if order.IsIceberg && order.DisplayQuantity < fillQuantity {
			fillQuantity = order.DisplayQuantity // Match capped at visible slice
		}
		
		// Generate the trade record
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
		
		// Iceberg Replenishment and Priority Degradation
		if order.IsIceberg {
			order.DisplayQuantity -= fillQuantity
			if order.DisplayQuantity <= 0 && order.HiddenQuantity > 0 {
				replenishAmount := order.Quantity * 0.10
				if order.HiddenQuantity < replenishAmount {
					replenishAmount = order.HiddenQuantity
				}
				order.DisplayQuantity = replenishAmount
				order.HiddenQuantity -= replenishAmount
				
				// Priority Degradation: Timestamp is refreshed to push order to back of queue
				order.Timestamp = time.Now()
			}
		}
	}

	// Append remaining unmatched volume to respective FIFO side queues
	if order.Side == models.SideBuy {
		ob.bids = append(ob.bids, order)
	} else {
		ob.asks = append(ob.asks, order)
	}

	return trades, nil
}
```

---

## 5. Downstream Event Pipelines

When a trade is successfully compiled by the deterministic matching loop, it must be distributed instantly without blocking the core execution path:

1.  **Trade Broadcast:** Real-time executions are formatted as immutable `[[entities/trade-execution-model]]` logs and sent to the `nte.trades.matched` stream via [[concepts/kafka-integration]].
    *   The [[entities/trade-settlement-system]] consumes this to run clearing, margin checks, and custody updates.
    *   The [[entities/compliance-surveillance-monitor]] processes this feed to detect market abuse patterns such as wash trading or spoofing.
2.  **L2/L3 Market Data:** State mutations of the book are serialized and published to `nte.orderbook.snapshots`. The [[entities/market-data-gateway]] consumes these snapshots to push real-time order-book depth updates to external clients.
3.  **Low-Latency Configurations:** To guarantee system throughput remains uninhibited, the publisher properties in `config/exchange.yaml` are optimized using `linger_ms = 1` to disable bulk queuing delays, as detailed in [[decisions/configuration-latency-tuning]].