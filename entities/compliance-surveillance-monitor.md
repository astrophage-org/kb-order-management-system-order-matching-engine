<!-- anchor: README.md:L1-L100 sha:HEAD -->

# Compliance Surveillance Monitor (CSM)

The **Compliance Surveillance Monitor (CSM)** is a real-time, out-of-band surveillance and regulatory engine designed under the Global Financial Markets Group (GFMG) standard. It asynchronously analyzes execution records and state changes produced by the [[summaries/order-matching-engine]] to identify manipulative trading practices. By consuming events from the message bus, the CSM operates without introducing execution latency into the core matching path.

---

## Responsibilities

The Compliance Surveillance Monitor protects the market's integrity by executing continuous, stateful heuristic analysis over the transaction stream. Its core responsibilities include:

*   **Real-time Event Ingestion:** Consuming matched trade records from the `nte.trades.matched` topic and execution exceptions/rejections from the `nte.orders.rejected` topic via [[concepts/kafka-integration]].
*   **Wash Trading Detection:** Analyzing incoming [[entities/trade-execution-model]] records to identify instances where the `BuyerID` and `SellerID` share the same beneficial ownership or originate from the same `TraderID` within sub-millisecond windows.
*   **Spoofing and Layering Detection:** Correlating highly transient order patterns—such as the rapid placement of large, non-bona fide orders (obtained via correlation of inbound orders and the `nte.orders.rejected` or cancellation streams) with order book depth modifications distributed to the [[entities/market-data-gateway]]—to identify quote manipulation.
*   **Alert Generation and Serialization:** Publishing standardized compliance alerts to downstream regulatory compliance channels and audit trails with precise microsecond-level latency stamps.
*   **Post-Trade Action Triggering:** Interfacing with risk gates to temporarily flag or suspend accounts exhibiting toxic or manipulative profiles.

---

## Dependencies

The CSM acts as a downstream consumer of the core exchange ecosystem. Its operation relies on the following key components:

*   **[[concepts/kafka-integration]] (Librdkafka Wrapper):** Inherits configuration patterns from `config/exchange.yaml` (e.g., partition assignment, symbol-keyed ordering) to guarantee that all execution logs for a specific financial symbol are processed in strict chronological order.
*   **[[entities/trade-execution-model]]:** Relies on the structured, immutable Go schema `TradeExecution` to deserialize events from the `nte.trades.matched` stream, analyzing critical properties such as price, execution phase (`CONTINUOUS` or `AUCTION`), and participant IDs.
*   **[[entities/matching-engine]]:** Indirectly relies on the upstream matching engine and its sharded [[entities/order-book]] structure to generate deterministic outputs free of race conditions or state gaps.
*   **[[entities/trade-settlement-system]] (TSS):** Works in parallel with TSS post-trade operations, ensuring that flagged trades can be held, investigated, or reported to clearinghouses prior to final settlement resolution.

---

## Architectural Position

To maintain the microsecond-level latency profile of the [[entities/matching-engine]], the CSM is physically and logically decoupled from the matching pipeline. It receives data asynchronously via high-throughput messaging:

```
┌────────────────────────────────────────┐
│      `Matching Engine` Core          │
└──────────────────┬─────────────────────┘
                   │
                   ▼ (Asynchronous Publish)
┌────────────────────────────────────────┐
│      Topic: nte.trades.matched         │
└──────────────────┬─────────────────────┘
                   │
                   ├──────────────────────────────────────┐
                   ▼                                      ▼
┌────────────────────────────────────────┐  ┌────────────────────────────────────┐
│     `Trade Settlement System`        │  │   `Compliance Surveillance` (CSM)│
│  - Clearing & Margin Verification      │  │  - Wash Trade Detection            │
│  - Custody Balance Updates             │  │  - Spoofing & Layering Heuristics  │
└────────────────────────────────────────┘  └────────────────────────────────────┘
```

---

## Detection Heuristics & Algorithms

### 1. Wash Trading Detection
Wash trading occurs when a market participant enters buy and sell orders for the same financial instrument to create misleading, artificial activity. 

The CSM monitors the stream for the following pattern:
$$\text{Symbol}_{T_x} = \text{Symbol}_{T_y} \quad \wedge \quad \text{BuyerID}_{T_x} = \text{SellerID}_{T_x}$$
Because of modern high-frequency algorithms, this check also extends to **Collusive Wash Trading**, where multiple associated accounts are rotated:

```
   [Trader Account A] ─── (Buy Order) ───┐
                                          ├─► `Order Book` ──► [Matched Trade Record]
   [Trader Account B] ─── (Sell Order) ──┘                           │
           ▲                                                         ▼
           └────────── (Same Beneficial Owner / IP) ◄───────── [CSM Alert Engine]
```

When a match is found in the `TradeExecution` payload, the CSM evaluates:
*   **Beneficial Owner Mapping:** Resolving `TraderID` fields to their parent clearing firms.
*   **Self-Match Interval:** Flagging executions occurring within microseconds where matching parties are tightly bound.

### 2. Spoofing and Layering
Spoofing involves submitting non-bona fide orders that the creator intends to cancel before execution, creating a false appearance of supply or demand to manipulate prices.

The CSM detects this by correlating state transitions:
1.  **Placement Phase:** A large limit order is placed far from the spread (or inside it), visible on L2/L3 market depth (monitored via [[entities/market-data-gateway]] snapshots).
2.  **Price Movement:** The order pushes the market price in a specific direction.
3.  **Cancellation Phase:** The order is canceled immediately (detected via the ingestion of cancellations or rejections) before execution can occur, while opposite-side trades are executed on a secondary account.

The heuristic engine tracks the ratio of **Order Volume Placed** to **Order Volume Executed** ($V_p / V_e$) within sliding time windows for each participant, raising anomalies when this ratio diverges sharply from historical baselines.