# Architectural Summary: Nexus Trading Exchange (NTE) Order Matching Engine

The **Nexus Trading Exchange (NTE) Order Matching Engine (OME)** is a high-performance, deterministic transaction processing core designed under the **Global Financial Markets Group (GFMG)** standard. The system is engineered to maintain ultra-low latency execution profiles while handling multi-asset trading volumes under strict regulatory and audit constraints.

This document synthesizes the high-level architecture, design patterns, component interactions, and data paths that define the OME.

---

## 1. System Context & Ecosystem Integration

The OME operates as the stateful, memory-centric nucleus of the NTE trading platform. To prevent the core matching process from being bottlenecked by network I/O or downstream persistence, the OME decouples ingestion and post-trade streaming via specialized peripheral systems:

*   **`Market Data Gateway` (MDG):** Manages external client sessions (translating protocols like FIX, FAST, or WebSocket). It ingests raw client requests to feed the engine's inbound channels, and conversely consumes market depth snapshots (`nte.orderbook.snapshots`) from the matching engine to distribute L2/L3 order book feeds.
*   **`Trade Settlement System` (TSS):** An asynchronous post-trade processing engine that consumes trade reports from the `nte.trades.matched` stream. TSS processes margin verification, clearing, clearinghouse routing, and custody balance updates.
*   **`Compliance Surveillance Monitor` (CSM):** A real-time compliance system that monitors the execution flow via `nte.trades.matched` and `nte.orders.rejected` to flags manipulative market patterns (such as wash trading, spoofing, and layering).

```
                  ┌──────────────────────────┐
                  │  `Market Data Gateway`  │
                  └─────────────┬────────────┘
                                │ (Inbound Orders)
                                ▼
┌────────────────────────────────────────────────────────┐
│             NTE Order Matching Engine (OME)            │
│                                                        │
│  ┌──────────────────┐    ┌───────────────┐    ┌──────┐ │
│  │ `Matching Engine`──>│ `Order Book`├───>│Egress│ │
│  └──────────────────┘    └───────────────┘    └──┬───┘ │
└──────────────────────────────────────────────────┼─────┘
                                                   │
                        ┌──────────────────────────┴────────────────────────┐
                        │ Topic: nte.trades.matched                         │ Topic: nte.orderbook.snapshots
                        ▼                                                   ▼
          ┌───────────────────────────┐                       ┌───────────────────────────┐
          │ `Trade Settlement System` │                     │  `Market Data Gateway`  │
          └─────────────┬─────────────┘                       └───────────────────────────┘
                        │
                        ▼
          ┌───────────────────────────┐
          │`Compliance Surveillance`│
          └───────────────────────────┘
```

---

## 2. Component Architecture

The internal execution architecture of the engine is separated into three distinct, decoupled execution layers:

```
       [ Client Inbound / Traffic Simulation ]
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ MAIN SERVER ENTRYPOINT (main.go)                        │
│ - Orchestrates life-cycle & graceful shutdown           │
│ - Resolves configurations (exchange.yaml)               │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ 1. INGRESS & ROUTING LAYER (`Matching Engine`)         │
│ - Shards incoming traffic by financial symbol           │
│ - Maintains concurrent map of symbol-specific books     │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ 2. DETERMINISTIC MATCHING CORE (`Order Book`)         │
│ - Executes Price-Time Priority logic                    │
│ - Unrolls `Iceberg Order Engine` Display vs Hidden    │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ 3. EGRESS LAYER (`Kafka Integration`)                 │
│ - Low-latency JSON serialization                       │
│ - Symbol-keyed Kafka partitioning                       │
└─────────────────────────────────────────────────────────┘
```

### A. Ingress & Routing Layer: ``Matching Engine``
The `Engine` manages the lifetime and routing of all asset-specific order books. It acts as an entry router:
*   Maintains a global registry map (`map[string]*OrderBook`) of active markets.
*   Implements a concurrent safe-read pattern using Go's `sync.RWMutex` to prevent race conditions during book initialization while allowing parallel, unblocked execution of independent symbol books.
*   Records microsecond-level telemetric latency stamps at the boundary of ingestion and processing.

### B. Deterministic Matching Core: ``Order Book``
The memory-resident limit order book core. This module executes execution-matching logic completely isolated from external dependencies:
*   **Price-Time Priority (FIFO):** Orders are stored in arrays representing bids and asks. Orders are filled sequentially based on the best price available, then by the earliest arrival timestamp.
*   **Instrument Isolation:** No global matching lock exists; each `OrderBook` executes matches within its own lock scope. Transactions in `BTC/USD` never block transactions in `ETH/USD`.
*   **`Iceberg Order Engine`:** Built-in native support for synthetic/hidden liquidity configurations, dividing large block orders into visible "display" slices and resting "hidden" pools.

### C. Egress Layer: ``Kafka Integration``
The outbound communication layer built upon a wrapper of `librdkafka` via `events.KafkaPublisher`:
*   Provides ultra-low latency JSON serialization.
*   Enforces deterministic keying strategies: Partitioning records by `Symbol` ensures that down-stream systems receive chronological updates for any single asset within a single, ordered Kafka partition.

---

## 3. Core Domain Abstractions & Models

The OME operates on two primary immutable data models that dictate the life cycle of a transaction:

### ``Order Model``
Defines an instruction to execute a transaction. Supports limit, market, and native iceberg variants.
*   **`Side`**: Binary execution state (`BUY` or `SELL`).
*   **`Type`**: Dictates matching paths (`LIMIT` or `MARKET`).
*   **Iceberg Fields**: `IsIceberg`, `DisplayQuantity`, and `HiddenQuantity` control fractional book exposure to protect market participants from exposing large blocks of institutional liquidity.

### ``TradeExecution Model``
The record of a finalized match. Whenever an incoming order crosses the spread and consumes resting limit liquidity, a trade is compiled.
*   Captures IDs for both buyer and seller, price, final fill quantity, matching phase (e.g., `CONTINUOUS`, `AUCTION`), and microsecond-level timestamps.
*   Acts as the immutable data source for downstream settlement, clearing, and surveillance.

---

## 4. Key Architectural & Design Decisions

### Split-Locking Patterns
To resolve the classical trade-off between concurrency safety and low-latency throughput, the OME employs split-locking:
1.  A global read-write lock (`sync.RWMutex`) protects the `Engine` registry. It is acquired briefly in read mode to locate the target `OrderBook` pointer, then instantly released.
2.  Each `OrderBook` manages its own internal mutex. Once the pointer is acquired, the matching loop locks the specific book. This allows high-concurrency throughput across thousands of symbols simultaneously.

### The `Iceberg Order Engine` Algorithm
Iceberg orders hide resting book volume to minimize adverse market impact. The execution logic handles this natively:
1.  The order is split into a visible `DisplayQuantity` and a `HiddenQuantity`.
2.  Matching routines only match against the active `DisplayQuantity` resting on the book.
3.  Once the visible slice is fully consumed, the matching core runs a replenishment step: it subtracts the next equivalent slice from `HiddenQuantity`, updates the `DisplayQuantity`, and appends the "newly visible" slice to the back of the queue at that price level (losing time priority for that slice, but preserving the hidden volume).

### `Crash Recovery and Durability`
Because state-mutations are executed purely in-memory to ensure microsecond-level latency, the system utilizes **Event Sourcing** for crash recovery:
*   Every inbound order is logged to an immutable Kafka log.
*   Upon startup, the engine consumes the inbound topic from the start of the trading window, re-playing messages chronologically to reconstruct the exact state of all limit order books.
*   End-of-day (EOD) snapshots are generated and persisted to cold storage to truncate replay boundaries and reduce recovery time objectives (RTO).

### `Configuration and Latency Tuning`
The configuration profile (`config/exchange.yaml`) is engineered to prioritize financial integrity alongside low-latency processing:
*   `kafka.producer_acks = "all"`: Prevents any data loss or divergence between the matching engine and settlement.
*   `kafka.linger_ms = 1`: Disables heavy batching delays, flushing execution reports to the wire almost instantly to keep downstream pipelines clear.

---

## 5. End-to-End Order Lifecycle Path

The following diagram trace the lifecycle of an order from inbound receipt to egress broadcast:

```
[Inbound Order Source]
        │
        │ 1. Ingests Order (e.g., BTC/USD Limit Order)
        ▼
 ┌──────────────────────┐
 │ `Matching Engine`  │
 └──────────┬───────────┘
            │
            │ 2. Acquires R-Lock to find Symbol Book
            │ 3. Routes Order to target OrderBook
            ▼
 ┌──────────────────────┐
 │   `Order Book`     │
 └──────────┬───────────┘
            │
            │ 4. Resolves Iceberg constraints (Display/Hidden Split)
            │ 5. Executes Match Logic (Price-Time Priority loop)
            │ 6. Appends unmatched balance to Rested Book Slices
            ▼
 ┌────────────────────────────────────────────────────────┐
 │ Execution Outputs                                      │
 └────┬──────────────────────────────────────────────┬────┘
      │ (Matched Trade Details)                      │ (Updated Book Depth)
      ▼                                              ▼
 ┌──────────────────────────────────────────────┐ ┌──────────────────────────────────────────────┐
 │ Topic: nte.trades.matched                    │ │ Topic: nte.orderbook.snapshots               │
 └──────────────────────────────────────────────┘ └──────────────────────────────────────────────┘
```

1.  **Ingestion:** An inbound client order is processed by the `Matching Engine`.
2.  **Lookup:** The engine looks up the matching `Order Book` in its registry and handovers execution to that specific book instance.
3.  **Processing:** The `Order Book` performs price-time priority matching. It executes any matches against resting liquidity, unrolling `Iceberg Order Engine` slices as necessary.
4.  **Reporting:** Executed transactions generate immutable `TradeExecution Model` structures. 
5.  **Distribution:** The outputs are sent to the egress layer via `Kafka Integration`, distributing execution records and book snapshots to downstream nodes like the `Trade Settlement System` and `Market Data Gateway`.