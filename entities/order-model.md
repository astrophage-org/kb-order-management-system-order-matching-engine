<!-- anchor: cmd/server/main.go:L1-L100 sha:HEAD -->

# Order Model

The **Order Model** is the core immutable domain abstraction representing a trader's instruction to buy or sell a financial instrument on the Nexus Trading Exchange (NTE). It is designed for microsecond-level serialization and zero-allocation routing through the [[entities/matching-engine]] and into the instrument-specific [[entities/order-book]].

By supporting Limit, Market, and native Iceberg configurations, the `Order` model serves as the state-defining payload from initial ingress translation at the [[entities/market-data-gateway]] to final execution.

---

## Responsibilities

*   **Instruction Standardization:** Formulates a deterministic, type-safe representation of external trade requests (e.g., FIX/WebSocket client protocols).
*   **Execution Strategy Support:** Encapsulates necessary state configurations for multiple matching paths:
    *   **Market Orders:** Aggressive instructions seeking immediate execution against the resting book at any price.
    *   **Limit Orders:** Rested instructions bound by strict price parameters, which are managed within the [[entities/order-book]] according to [[concepts/price-time-priority]].
    *   **Iceberg Orders:** Advanced synthetic configurations that split massive institutional block sizes into visible "display" and hidden "replenishment" pools to minimize adverse market impact (managed via the [[concepts/iceberg-order-engine]]).
*   **Low-Latency Serialization:** Tailored for efficient JSON serialization to enable high-throughput transit via [[concepts/kafka-integration]] and high-performance reconstruction during [[concepts/crash-recovery-durability]] event replays.

---

## Dependencies

*   **Consumer - [[entities/matching-engine]]:** Ingests, validates, and routes the order structure to the correct symbol-sharded book.
*   **Processor - [[entities/order-book]]:** Evaluates order fields against the active spread, matching incoming commands or appending them to the price-priority queues.
*   **Logic Extension - [[concepts/iceberg-order-engine]]:** Dynamically manipulates an iceberg order's `DisplayQuantity` and `HiddenQuantity` fields during replenishment routines.
*   **Serialization - [[concepts/kafka-integration]]:** Enforces schema compatibility for symbol-keyed partition distribution on Kafka topics like `nte.orders.inbound` and `nte.orders.rejected`.

---

## Data Model Definition

The physical structure of an order is defined in Golang as follows:

```go
package models

import "time"

type Side string
type OrderType string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"

	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeMarket OrderType = "MARKET"
)

// Order represents an inbound request from the market-data-gateway/broker.
type Order struct {
	ID              string    `json:"id"`
	Symbol          string    `json:"symbol"`
	TraderID        string    `json:"trader_id"`
	Side            Side      `json:"side"`
	Type            OrderType `json:"type"`
	Price           float64   `json:"price,omitempty"`
	Quantity        float64   `json:"quantity"`
	IsIceberg       bool      `json:"is_iceberg"`                 // Indicates if this is an Iceberg order
	DisplayQuantity float64   `json:"display_quantity,omitempty"` // For Iceberg orders, the visible portion shown to market
	HiddenQuantity  float64   `json:"hidden_quantity,omitempty"`  // For Iceberg orders, the remaining hidden portion
	Timestamp       time.Time `json:"timestamp"`
}
```

### Field Specifications

| Field Name | Type | Description | Optimization / Constraint |
| :--- | :--- | :--- | :--- |
| `ID` | `string` | Globally unique identifier (typically UUIDv4). | Used as the correlation ID across downstream boundaries. |
| `Symbol` | `string` | The financial instrument (e.g., `BTC/USD`). | Serves as the routing key in the [[entities/matching-engine]] and the partition key in [[concepts/kafka-integration]]. |
| `TraderID` | `string` | Identifies the originating client account. | Crucial for clearing, settlement, and verification checks. |
| `Side` | `Side` | Binary action: `BUY` or `SELL`. | Governs which queue inside the [[entities/order-book]] the order enters. |
| `Type` | `OrderType` | Execution instruction style: `LIMIT` or `MARKET`. | Selects the matching logic execution path. |
| `Price` | `float64` | The limit execution price threshold. | Omitted for Market Orders. Represented as floating-point but validated against minimum ticks. |
| `Quantity` | `float64` | Total order volume requested. | Must be greater than zero and conform to `max_order_size` in the system configuration. |
| `IsIceberg` | `bool` | Flag denoting whether the order is an Iceberg layout. | If `true`, the matching engine invokes synthetic replenishment routines. |
| `DisplayQuantity` | `float64` | The current slice of quantity visible to the public L2/L3 books. | Managed dynamically by the [[concepts/iceberg-order-engine]]. |
| `HiddenQuantity` | `float64` | Remaining un-displayed balance resting on the hidden queue. | Not visible on L2/L3 books; holds no priority over visible orders. |
| `Timestamp` | `time.Time` | Microsecond-precision ingestion timestamp. | Determines initial queue priority under [[concepts/price-time-priority]]. |

---

## Lifecycle within the Matching Core

```
       Inbound Client Request (FIX/WS)
                     │
                     ▼
       ┌───────────────────────────┐
       │ `Market Data Gateway`   │ (Translation to JSON Order)
       └─────────────┬─────────────┘
                     │
                     ▼
       ┌───────────────────────────┐
       │   `Matching Engine`     │ (Routes by Symbol)
       └─────────────┬─────────────┘
                     │
                     ▼
       ┌───────────────────────────┐
       │     `Order Book`        │ (Evaluates against the spread)
       └─────────────┬─────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
   [Crosses Spread]       [No Cross / Limit]
         │                       │
         │ (Match occurs)        │ (Resting Order)
         ▼                       ▼
 ┌───────────────┐       ┌───────────────────────────────┐
 │ Generates     │       │ Added to Bid/Ask Queue        │
 │ `TradeExecution Model`│       │ (Under `Price-Time Priority`)│
 └───────────────┘       └───────────────┬───────────────┘
                                         │
                                         ▼
                         ┌───────────────────────────────┐
                         │ If `IsIceberg == true`:       │
                         │ Replenish via                 │
                         │ `Iceberg Order Engine`      │
                         └───────────────────────────────┘
```

1. **Ingress Translation:** The [[entities/market-data-gateway]] receives external client events, converts them to the JSON-compatible `Order` model representation, and streams them into the matching core.
2. **Routing:** The [[entities/matching-engine]] parses the `Symbol` field, acquires a split-lock read handle (as described in the [[decisions/split-locking-pattern]]), and passes the model to the target [[entities/order-book]].
3. **Execution Logic:**
   * **Market Order:** Evaluates current liquidity. It instantly consumes the necessary amount, generating a [[entities/trade-execution-model]] for downstream consumption, and rejects any remaining unfilled portion.
   * **Limit Order:** If the price crosses the spread, it generates immediate fills. Unfilled balances are appended to the internal execution queue at that price level.
   * **Iceberg Order:** Evaluated strictly using the current `DisplayQuantity`. When the active display is exhausted, the [[concepts/iceberg-order-engine]] automatically decreases the `HiddenQuantity`, creates a new visible slice, and appends it to the end of the time priority queue at that price point.
4. **Persisted Log Replay:** To support zero-loss restarts, instances of the `Order` model are read from historical Kafka partitions during [[concepts/crash-recovery-durability]] operations to rebuild memory-resident books back to their exact, pre-crash state.