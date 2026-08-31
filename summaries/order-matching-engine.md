<!-- anchor: README.md:L1-L100 sha:HEAD -->

# NTE Order Matching Engine (OME) Architectural Summary

The **Nexus Trading Exchange (NTE) Order Matching Engine (OME)** is the ultra-low latency, deterministic transaction processing core of the exchange. Operating under the **Global Financial Markets Group (GFMG)** standard, the OME is engineered to execute high-volume multi-asset orders with microsecond-level latency while ensuring absolute transactional integrity, deterministic recovery, and strict post-trade audit compliance.

This document outlines the high-level architecture, component communication patterns, core data models, and critical design patterns defining the OME. For a wider view of the platform, see the [[index]].

---

## 1. System Ecosystem & Integration Context

The OME acts as a stateful, in-memory execution nucleus. To protect the matching core from network overhead, protocol translation delays, or slow database transactions, it is entirely decoupled from peripheral components. It communicates with external systems using high-throughput, low-latency queues managed via [[concepts/kafka-integration]].

```
                  ┌─────────────────────────────┐
                  │  [[entities/market-data-gateway]]  │
                  └─────────────┬───────────────┘
                                │ (Inbound Orders)
                                ▼
┌────────────────────────────────────────────────────────┐
│             NTE Order Matching Engine (OME)            │
│                                                        │
│  ┌─────────────────────────┐    ┌──────────────────┐  │
│  │ [[entities/matching-engine]] ───>│[[entities/order-book]]│  │
│  └─────────────────────────┘    └────────┬─────────┘  │
└──────────────────────────────────────────┼─────────────┘
                                           │ (Trades & Snaps)
                                           ▼
                 ┌────────────────────────────────────────┐
                 │          Kafka Event Broker            │
                 └──────────┬──────────────────┬──────────┘
                            │                  │
 (Matched Trades)           ▼                  ▼ (L2/L3 Book Snapshots)
    ┌───────────────────────────┐          ┌─────────────────────────────┐
    │[[entities/trade-settlement-system]]│          │  [[entities/market-data-gateway]]  │
    └───────────┬───────────────┘          └─────────────────────────────┘
                │
 (Matched Trades)│
                ▼
    ┌───────────────────────────────┐
    │ [[entities/compliance-surveillance-monitor]] │
    └───────────────────────────────┘
```

The OME integrates directly with three crucial sister systems:
*   **[[entities/market-data-gateway]] (MDG):** Ingests client traffic (such as FIX/WebSocket/REST protocols), validates parameters, and streams normalized messages to the OME via `nte.orders.inbound`. MDG also consumes output from `nte.orderbook.snapshots` to reconstruct and distribute real-time L2/L3 order book feeds back to market participants.
*   **[[entities/trade-settlement-system]] (TSS):** Consumes trade reports asynchronously from the `nte.trades.matched` stream. TSS manages margin validations, clearinghouse clearing routines, and cold-storage custody balance updates.
*   **[[entities/compliance-surveillance-monitor]] (CSM):** Taps into `nte.trades.matched` and `nte.orders.rejected` in real time. CSM utilizes heuristic analysis to flag market abuse behaviors, including wash trading, spoofing, and layering.

---

## 2. Layered Component Architecture

The internal design of the OME is split into three decoupled execution layers: Ingress & Routing, Deterministic Matching Core, and Egress & Serialization.

```
                  [ Inbound Order Stream ]
                             │
                             ▼
 ┌───────────────────────────────────────────────────────┐
 │ 1. INGRESS & ROUTING LAYER                            │
 │    - Symbol Sharding Map                              │
 │    - [[entities/matching-engine]] Entrypoint          │
 └───────────────────────────┬───────────────────────────┘
                             │ (Routed Order Object)
                             ▼
 ┌───────────────────────────────────────────────────────┐
 │ 2. DETERMINISTIC MATCHING CORE                        │
 │    - [[concepts/price-time-priority]] (FIFO Engine)   │
 │    - [[entities/order-book]] Instances                │
 │    - [[concepts/iceberg-order-engine]] Replenisher    │
 └───────────────────────────┬───────────────────────────┘
                             │ (Unserialized Outputs)
                             ▼
 ┌───────────────────────────────────────────────────────┐
 │ 3. EGRESS & SERIALIZATION LAYER                       │
 │    - [[concepts/kafka-integration]]                   │
 │    - Low-latency JSON Encoder                         │
 └───────────────────────────────────────────────────────┘
```

### A. Ingress & Routing Layer: `[[entities/matching-engine]]`
Managed by the `Engine` structure (defined in `internal/matching/engine.go`), this layer processes incoming transaction streams:
*   **Symbol Sharding:** It acts as an entry-router that manages a global concurrent map (`map[string]*OrderBook`) representing active instrument trading arenas.
*   **Contention Management:** Instantiates and uses Go's `sync.RWMutex` to prevent data races during dynamic instrument book initialization while keeping normal transaction routing lock-free at the engine-level.
*   **Microsecond Latency Stamping:** Records high-resolution entry timestamps to track telemetry and measure execution processing times.

### B. Deterministic Matching Core: `[[entities/order-book]]`
An in-memory, stateful limit order book managed via individual `OrderBook` structs:
*   **Execution Isolation:** Each asset (e.g., `BTC/USD`) runs its own order book inside an isolated execution lock context. This eliminates global matching bottlenecks, allowing highly concurrent throughput across unrelated books.
*   **Matching Algorithm:** Leverages a strict [[concepts/price-time-priority]] matching queue (FIFO) to clear crossed bids and asks sequentially.
*   **Hidden Liquidity Processing:** Employs the [[concepts/iceberg-order-engine]] to manage synthetic block transactions, dividing hidden and visible quantities, and executing replenishment steps on the FIFO queues.

### C. Egress & Serialization Layer: `[[concepts/kafka-integration]]`
Encapsulated inside the `events.KafkaPublisher` wrapper around `librdkafka`:
*   **Deterministic Keying:** Keys all published snapshots and trades by their specific `Symbol`, ensuring that downstream systems consume chronological events for a given asset within a single, ordered Kafka partition.
*   **Minimal Serialization Overhead:** Executes lightning-fast structural JSON serialization of matched events to preserve microsecond execution windows.

---

## 3. Core Domain Models

The matching core operates on two primary immutable schemas:

### `[[entities/order-model]]`
Represents an instruction to buy or sell a specified quantity of an asset.
*   **Identifiers:** Uniquely maps orders using `ID`, `Symbol`, and `TraderID`.
*   **Execution Directives:** Supports `Side` (BUY/SELL) and `Type` (LIMIT/MARKET) flags.
*   **Synthetic Configurations:** Contains boolean fields for `IsIceberg`, along with `DisplayQuantity` and `HiddenQuantity` fields. These configurations allow institutional traders to hide large blocks of liquidity, and they are parsed natively by the [[concepts/iceberg-order-engine]].

### `[[entities/trade-execution-model]]`
Represents the immutable record of a completed trade. Generated whenever an incoming order crosses the bid-ask spread of a resting [[entities/order-book]].
*   Captures IDs of both execution parties (`BuyerID`, `SellerID`, `BuyOrderID`, `SellOrderID`), execution price, matched volume, matching phase (`CONTINUOUS` or `AUCTION`), and high-resolution timestamps.
*   Acts as the source of truth for the [[entities/trade-settlement-system]] and the [[entities/compliance-surveillance-monitor]].

---

## 4. Key Architectural & Design Decisions

### Split-Locking Pattern
To scale throughput across thousands of independent financial markets, the system implements the [[decisions/split-locking-pattern]]:
1.  A global read-write mutex (`sync.RWMutex`) is held by the [[entities/matching-engine]] registry. It is acquired briefly in read mode (`RLock()`) to look up the pointer of an active [[entities/order-book]].
2.  Once the pointer is resolved, the global read lock is immediately released.
3.  The engine locks the individual, isolated mutex of that specific [[entities/order-book]]. As a result, heavy order book updates in `BTC/USD` never block concurrent transaction processing for `ETH/USD` or other symbols.

### Synthetic Liquidity Execution
Through the integrated [[concepts/iceberg-order-engine]], the system natively processes hidden fractions of large block trades:
1.  An iceberg order is split into visible (`DisplayQuantity`) and hidden (`HiddenQuantity`) components.
2.  Only the active display slice is placed in the public-facing priority queue of the [[entities/order-book]] to protect the execution from adverse market impacts.
3.  When the resting display slice is fully exhausted, the system runs an automated replenishment cycle. This step subtracts the next equivalent slice from `HiddenQuantity`, updates `DisplayQuantity`, and appends the new slice to the tail of the matching queue at that price level (thus losing time priority for that slice, but preserving the hidden reserve).

### Crash Recovery and Durability Stategy
Because OME states reside entirely in memory to achieve ultra-low latencies, the system uses an event-sourcing model for recovery:
*   The [[concepts/crash-recovery-durability]] strategy requires writing every incoming order to an immutable Kafka log.
*   In the event of a system crash, the OME rebuilds its internal memory state by consuming the inbound partition offset from the beginning of the trading session. It replays these transactions chronologically to reconstruct the exact state of the [[entities/order-book]].
*   End-of-day (EOD) snapshots of the order books are compiled and persisted to secure cold storage, truncating the replay boundary and reducing the Recovery Time Objective (RTO).

### Latency Optimization Tuning
The OME is tuned to balance performance with financial integrity using specific profiles defined in `config/exchange.yaml`:
*   **Deterministic Persistence:** `kafka.producer_acks` is configured to `all` to ensure no divergence occurs between the matching engine and downstream settlement networks.
*   **Zero Linger Latency:** Utilizing [[decisions/configuration-latency-tuning]], the producer profile configures `kafka.linger_ms = 1`. This disables heavy transmission batching, flushing executed records immediately to downstream consumers to keep network interfaces clear.

---

## 5. End-to-End Order Execution Flow

```
                                [ CLIENT ORDER ]
                                       │
                                       ▼
                     ┌───────────────────────────────────┐
                     │ [[entities/market-data-gateway]]  │
                     └─────────────────┬─────────────────┘
                                       │ (Kafka Inbound)
                                       ▼
                     ┌───────────────────────────────────┐
                     │   [[entities/matching-engine]]    │
                     └─────────────────┬─────────────────┘
                                       │ (Engine Registry Map Lookups)
                                       ▼
                     ┌───────────────────────────────────┐
                     │     [[entities/order-book]]       │
                     └─────────────────┬─────────────────┘
                                       ├─> [ Is Iceberg? ]
                                       │   ├─ Yes: Run [[concepts/iceberg-order-engine]] Split
                                       │   └─ No:  Standard Path
                                       ▼
                     ┌───────────────────────────────────┐
                     │ [[concepts/price-time-priority]]  │
                     └─────────────────┬─────────────────┘
                                       │ (Execution Match Processed)
                                       ├──────────────────────────────────────┐
                                       │ (Matched Trade Details)              │ (Updated Book Depth)
                                       ▼                                      ▼
                     ┌───────────────────────────────────┐  ┌───────────────────────────────────┐
                     │  Topic: nte.trades.matched        │  │  Topic: nte.orderbook.snapshots   │
                     └─────────────────┬─────────────────┘  └─────────────────┬─────────────────┘
                                       ▼                                      ▼
                     ┌───────────────────────────────────┐  ┌───────────────────────────────────┐
                     │[[entities/trade-settlement-system]]│  │ [[entities/market-data-gateway]]  │
                     └─────────────────┬─────────────────┘  └───────────────────────────────────┘
                                       ▼
                     ┌───────────────────────────────────┐
                     │[[entities/compliance-surveillance-monitor]]│
                     └───────────────────────────────────┘
```

1.  **Ingress Delivery:** The client transmits a transaction instruction. The [[entities/market-data-gateway]] parses the request and streams an [[entities/order-model]] to the matching engine.
2.  **Lookup & Routing:** The global [[entities/matching-engine]] acquires a brief read lock to look up the target [[entities/order-book]] in its symbol registry, then routes the transaction to the matching queue.
3.  **Book Mutex Acquisition:** The engine acquires the isolated mutex of the destination [[entities/order-book]] to process the order.
4.  **Spread & Matching Checks:** The order is evaluated against active resting liquidity. If the incoming order is an iceberg, the [[concepts/iceberg-order-engine]] splits the order into its visible and hidden components.
5.  **FIFO Priority Match Execution:** Matching processes are executed against the book's resting liquidity according to [[concepts/price-time-priority]] rules. 
6.  **Replenishment Execution:** If an iceberg display slice is exhausted, the engine replenishes it from the hidden pool and appends the new slice to the tail of the priority queue. Unmatched standard order balances are added directly to the active bid/ask lists.
7.  **Match Distribution:** Resulting matches generate immutable [[entities/trade-execution-model]] instances. These are serialized into JSON and dispatched to `nte.trades.matched` via [[concepts/kafka-integration]] to be consumed by the [[entities/trade-settlement-system]] and the [[entities/compliance-surveillance-monitor]].
8.  **Depth Snapshot Broadcast:** Updated book depth summaries are published to `nte.orderbook.snapshots`. The [[entities/market-data-gateway]] processes these messages to update public L2/L3 market data feeds.