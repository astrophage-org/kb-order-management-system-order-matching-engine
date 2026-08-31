# Iceberg Order Engine

The **Iceberg Order Engine** provides native, high-performance support for synthetic and hidden liquidity within the Nexus Trading Exchange (NTE). By dividing large block orders into visible "display" slices and resting "hidden" pools, the engine allows institutional participants to execute large-scale volume while minimizing adverse market impact and preventing front-running.

This capability is implemented natively within the in-memory [[entities/order-book]] to eliminate routing overhead and guarantee deterministic execution latency under the strict rules of [[concepts/price-time-priority]].

---

## 1. Concepts & Liquidity Architecture

An Iceberg order represents a single logical trade instruction (modeled via [[entities/order-model]]) split dynamically into two distinct liquidity layers:

```
┌─────────────────────────────────────────────────────────────┐
│                 Total Iceberg Order Volume                 │
└──────────────────────────────┬──────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
   ┌─────────────────────────┐   ┌─────────────────────────┐
   │    Display Quantity     │   │     Hidden Quantity     │
   │  (Synthetic Liquidity)  │   │   (Hidden Liquidity)    │
   └────────────┬────────────┘   └────────────┬────────────┘
                │                             │
                ▼                             ▼
     Broadcasted to L2 feeds        Held in-memory; invisible
     via `Market Data Gateway`     to market-data feeds
```

### Synthetic Liquidity (Display Quantity)
The active, visible fraction of the order currently resting on the order book. 
* It occupies a specific price level and is subject to standard [[concepts/price-time-priority]] queue rules.
* Updates to this quantity are broadcasted to the [[entities/market-data-gateway]] via the `nte.orderbook.snapshots` Kafka topic.

### Hidden Liquidity (Hidden Quantity)
The remaining, unexposed volume of the order.
* It is stored in-memory within the isolated [[entities/order-book]] instance.
* It is completely invisible to all public market data feeds, ensuring that external participants cannot detect the true depth of the resting book.
* It does not participate in matching until it is formally promoted to the visible queue during a replenishment cycle.

---

## 2. Managing Display & Hidden Fractions

When an Iceberg order is ingested by the [[entities/matching-engine]], it is evaluated and validated before being routed to the corresponding symbol book.

### Fractional Partitioning Mechanics
The engine initializes the iceberg properties according to the following rules (as implemented in `internal/matching/order_book.go`):

1. **Validation**: The total `Quantity` must be greater than zero.
2. **Expose Slicing**: If `IsIceberg` is enabled, the engine verifies the `DisplayQuantity`.
   * If `DisplayQuantity` is not explicitly configured (or is `<= 0`), the engine applies a default exposure slice of **10%** of the total quantity:
     $$\text{DisplayQuantity} = \text{Quantity} \times 0.10$$
3. **Hidden Quantity Calculation**: The hidden portion is calculated as the remainder of the total volume:
     $$\text{HiddenQuantity} = \text{Quantity} - \text{DisplayQuantity}$$

```go
// Pre-process Iceberg Orders inside OrderBook.AddOrder
if order.IsIceberg {
    // Initialize the display and hidden portions
    if order.DisplayQuantity <= 0 {
        order.DisplayQuantity = order.Quantity * 0.10 // default 10% visible
    }
    order.HiddenQuantity = order.Quantity - order.DisplayQuantity
}
```

---

## 3. Replenishment Execution Flow

The core of the Iceberg engine is its deterministic replenishment cycle. When an aggressive order matches against a resting Iceberg order, the engine executes matches against the visible slice first. Once that slice is exhausted, it triggers an immediate replenishment from the hidden pool.

### Step-by-Step Execution Walkthrough

```
[ Aggressive Inbound Order ]
            │
            ▼
┌──────────────────────────────┐ No
│  Does resting Order match?   ├────────► [ Skip / Rest on Book ]
└───────────┬──────────────────┘
            │ Yes
            ▼
┌──────────────────────────────┐
│ Match against resting        │
│ DisplayQuantity slice        │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│ Generate `TradeExecution Model` │
│ and subtract filled volume   │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│    Is DisplayQuantity = 0?   ├─ No ───► [ Complete Match Step ]
└───────────┬──────────────────┘
            │ Yes
            ▼
┌──────────────────────────────┐
│  Is there HiddenQuantity?    ├─ No ───► [ Order Fully Filled ]
└───────────┬──────────────────┘
            │ Yes
            ▼
┌──────────────────────────────┐
│  Replenishment Phase:        │
│  - Slice volume from Hidden  │
│  - Reset DisplayQuantity     │
│  - Re-enqueue to Book Tail   │  <─── (Loses Time Priority for this slice)
└──────────────────────────────┘
```

#### Step 1: Matching the Display Slice
Matches are strictly executed against the active `DisplayQuantity` resting on the book. An incoming counter-order cannot "penetrate" into the hidden portion directly; it can only consume what is currently made synthetic/visible.

#### Step 2: Slice Exhaustion
If the matched volume matches or exceeds the current `DisplayQuantity`, the display slice is reduced to zero. An immutable [[entities/trade-execution-model]] is instantly generated for the filled volume.

#### Step 3: Replenishment Trigger
Upon detection that `DisplayQuantity == 0` and `HiddenQuantity > 0`, the engine executes a synchronous replenishment block:
1. It calculates the next display slice size (configured as a fixed volume or a percentage of the initial total).
2. It subtracts this slice size from `HiddenQuantity`.
3. It sets `DisplayQuantity` to this slice size.
4. It shifts the replenished portion of the order to the **tail of the queue** at that price level.

#### Step 4: Time Priority Penalty
By replenishing and moving to the back of the price level queue, the newly visible slice receives a new timestamp. This preserves the integrity of [[concepts/price-time-priority]]: existing resting limit orders at that price level retain priority over the newly materialized iceberg slice.

### Code Implementation Reference
The code block below (from `internal/matching/order_book.go`) demonstrates the inline matching and replenishment logic:

```go
// Match up to the visible amount first
fillQuantity := order.Quantity / 2
if order.IsIceberg && order.DisplayQuantity < fillQuantity {
    fillQuantity = order.DisplayQuantity 
}

// ... Execute match and build trade execution record ...

// Iceberg replenishment logic
if order.IsIceberg {
    // Subtract from display first
    order.DisplayQuantity -= fillQuantity
    if order.DisplayQuantity <= 0 && order.HiddenQuantity > 0 {
        // Replenish from hidden
        replenishAmount := order.Quantity * 0.10 // Next 10% slice
        if order.HiddenQuantity < replenishAmount {
            replenishAmount = order.HiddenQuantity
        }
        order.DisplayQuantity = replenishAmount
        order.HiddenQuantity -= replenishAmount
    }
}
```

---

## 4. Architectural Considerations & Integration

### Downstream Messaging & Serialization
To avoid leakage of hidden liquidity data:
* **To [[entities/trade-settlement-system]] (TSS) and [[entities/compliance-surveillance-monitor]] (CSM):** Full execution reports, containing the underlying Iceberg configuration and exact trade details, are serialized into JSON and written to the `nte.trades.matched` topic via the [[concepts/kafka-integration]] layer.
* **To [[entities/market-data-gateway]] (MDG):** Only the updated **visible** quantities at each price level are published to the `nte.orderbook.snapshots` topic. The MDG receives no information regarding the remaining `HiddenQuantity`.

### Performance & Locking
Because the replenishment step alters order book priority structures, it must be performed safely. The NTE matching engine resolves thread contention using the [[decisions/split-locking-pattern]]:
* The global engine read-write lock (`sync.RWMutex`) is bypassed once the active symbol's [[entities/order-book]] pointer is located.
* The book-specific mutex is held during the entire matching and replenishment sequence, ensuring that no concurrent orders can disrupt the replenishment math or bypass priority updates.
* With [[decisions/configuration-latency-tuning]] applied (such as `linger.ms = 1` and zero-batching), the resulting trade executions and updated market depth snapshots are pushed to Kafka within microseconds of the replenishment.

### Resiliency and Determinism
During system recovery, the state of all Iceberg orders is fully reconstructed using Event Sourcing via [[concepts/crash-recovery-durability]]:
1. The engine replays the raw inbound orders from the Kafka log.
2. Because the replenishment logic is entirely deterministic, replaying the same sequence of inbound orders naturally reconstructs the exact same display and hidden fractions, matching the pre-crash book state precisely without requiring state synchronization from database backends.

---

## See Also
* [[entities/order-book]] — In-memory limit order books executing this replenishment logic.
* [[concepts/price-time-priority]] — The queuing algorithm governing display slice priority.
* [[decisions/split-locking-pattern]] — Thread safety strategy for symbol-specific matching engines.
* [[concepts/crash-recovery-durability]] — State recovery details for synthetic order types.