<!-- anchor: README.md:L1-L100 sha:HEAD -->

# Market Data Gateway

The **Market Data Gateway (MDG)** represents the edge boundary of the Nexus Trading Exchange (NTE) ecosystem. It acts as a high-throughput, low-latency interface that translates external client-facing trading protocols—such as FIX, FAST, and WebSockets—into the internal formats required by the [[summaries/order-matching-engine]]. Concurrently, the MDG ingests internal order book updates to synthesize and distribute Level 2 (L2 - aggregated price levels) and Level 3 (L3 - individual order-by-order detail) market data feeds to external market participants.

---

## ## Responsibilities

*   **External Protocol Translation (Ingress):** Converts incoming external client requests—such as FIX `NewOrderSingle (MsgType=D)` or WebSocket JSON payloads—into normalized internal `[[entities/order-model]]` Go structures.
*   **Order Routing Pipeline:** Enqueues parsed inbound transactions into the `nte.orders.inbound` topic via [[concepts/kafka-integration]], utilizing symbol-keyed partitioning to preserve chronological order per asset.
*   **Client Session Management:** Manages stateful client connections, encompassing FIX session life cycles (heartbeats, sequence resets, resend requests) and high-concurrency WebSocket subscription states.
*   **Market Data Synthesis (Egress):** Consumes raw delta snapshots from the `nte.orderbook.snapshots` Kafka topic, reconstructs consolidated order book states, and builds L2 and L3 market data feeds.
*   **Broadcasting:** Delivers real-time data streams to clients using high-performance protocols like FAST (FIX Adapted for Streaming) and low-overhead WebSockets.
*   **SLA and Telemetry Injection:** Injects ingress microsecond-level timestamps onto incoming packets to facilitate comprehensive latency and SLA tracking through the [[entities/matching-engine]] and downstream egress logs.

---

## ## Dependencies

*   **[[concepts/kafka-integration]]:** Relies on `librdkafka` configurations (utilizing low `linger.ms` settings as discussed in [[decisions/configuration-latency-tuning]]) to publish to `nte.orders.inbound` and consume from `nte.orderbook.snapshots`.
*   **[[entities/order-book]]:** The format of incoming limit and iceberg configurations must map directly to the structural limits of the matching core.
*   **[[entities/order-model]]:** Translates incoming network bytes directly into the fields (e.g., `DisplayQuantity`, `HiddenQuantity`, `IsIceberg`) required by the [[concepts/iceberg-order-engine]].
*   **Downstream Validation Systems:** Coordinates indirectly with the [[entities/trade-settlement-system]] and [[entities/compliance-surveillance-monitor]] by maintaining precise sequence numbers and audit records on all inbound order requests.

---

## Architectural Position

The Market Data Gateway acts as the gateway to the core transaction loop of the exchange:

```
                                  +─────────────────────────+
                                  | External Client Space   |
                                  +───┬─────────────────▲───+
       FIX / WebSocket (Inbound)      │                 │  WebSocket / FAST (L2/L3 Outbound)
                                      ▼                 │
     +──────────────────────────────────────────────────┴──────────────────────────────────+
     |                       `Market Data Gateway` (MDG)                                 |
     |                                                                                     |
     |   +─────────────────────────+                         +─────────────────────────+   |
     |   |   Ingress Parser        |                         |    Egress Synthesizer   |   |
     |   | (FIX/WS -> Order Model) |                         |  (Snapshots -> L2/L3)   |   |
     |   +───────────┬─────────────+                         +─────────────▲───────────+   |
     +───────────────┼─────────────────────────────────────────────────────┼───────────────+
                     │                                                     │
                     │ (nte.orders.inbound)                                │ (nte.orderbook.snapshots)
                     ▼                                                     │
     +─────────────────────────────────────────────────────────────────────┴───────────────+
     |                        [[concepts/kafka-integration]]                               |
     +───────────────┬─────────────────────────────────────────────────────────────────────+
                     │
                     ▼
     +─────────────────────────────────────────────────────────────────────────────────────+
     |                    [[summaries/order-matching-engine]]                              |
     +─────────────────────────────────────────────────────────────────────────────────────+
```

---

## Ingress Operations: Protocol Normalization

External clients communicate using standardized financial formats. The MDG normalizes these protocols into the internal execution models.

### 1. FIX (Financial Information eXchange) Ingress
The gateway runs a multi-threaded acceptor engine (supporting FIX 4.2, 4.4, and FIXT 1.1). When a client transmits a `NewOrderSingle (35=D)`, the parser maps the tag-value structure directly to the `models.Order` Go structure:

| FIX Tag | Field Name | Description / Normalization |
| :--- | :--- | :--- |
| `11` | `ClOrdID` | Mapped to `Order.ID` (converted to UUID format if necessary). |
| `55` | `Symbol` | Normalized to internal symbol pairs (e.g., `BTC/USD`). |
| `54` | `Side` | `1` = `SideBuy`, `2` = `SideSell`. |
| `40` | `OrdType` | `1` = `OrderTypeMarket`, `2` = `OrderTypeLimit`. |
| `44` | `Price` | Parsed to `float64` (multiplied by tick size constraints). |
| `38` | `OrderQty` | Total quantity. Mapped to `Order.Quantity`. |
| `10845` | `DisplayQty` | (Custom Tag) Mapped to `Order.DisplayQuantity`. Activates [[concepts/iceberg-order-engine]]. |

### 2. WebSocket JSON Ingress
For retail users and programmatic API integrations, WebSockets provide a direct route. The incoming JSON schemas are directly decoded into memory using high-performance parsing libraries to eliminate heap allocation overhead:

```json
{
  "action": "ORDER_CREATE",
  "cl_ord_id": "90af2d4b-9721-4d31-92be-e81bfb2649db",
  "symbol": "BTC/USD",
  "side": "BUY",
  "type": "LIMIT",
  "price": 65432.10,
  "quantity": 50.0,
  "is_iceberg": true,
  "display_quantity": 5.0
}
```

This JSON structure map-decodes directly into `models.Order`, setting the iceberg flags to hide structural size before the order enters the matching loop.

### 3. Ingestion Routing
Once parsed, the message is passed to a high-speed Kafka publisher wrapper:
*   **Partitioning Key:** Set deterministically using the order's `Symbol` string (e.g. `BTC/USD`). This ensures that all transactions for a given market are written to the same partition, guaranteeing chronological processing in downstream [[entities/order-book]] instances.
*   **Write Guarantee:** The MDG utilizes producer settings defined in [[decisions/configuration-latency-tuning]] (`acks=all`) to prevent order loss before client acknowledgment.

---

## Egress Operations: Market Data Distribution

The MDG reconstructs internal order states to serve clients with real-time trading visibility.

### L2 (Level 2) Aggregated Depth
Level 2 represents the aggregate volume available at distinct price bands up to a defined depth limit (typically 10, 20, or 50 levels deep).
1.  The gateway consumes raw serialized representations of books from the `nte.orderbook.snapshots` Kafka topic.
2.  It converts order list arrays into grouped structures:
    $$\text{L2 Depth} = \left\{ P_{\text{level}}, \sum Q_{\text{visible}} \right\}$$
3.  **Iceberg Handling:** Because of rules set in the [[concepts/iceberg-order-engine]], only `DisplayQuantity` is populated in the aggregate level calculation. Rested `HiddenQuantity` is omitted from the L2 broadcast feeds to prevent institutional footprint exposure.
4.  The structured snapshot is serialized using **FAST** or packed into JSON buffers for transmission over the public WebSocket channel.

### L3 (Level 3) Order-by-Order Feeds
For advanced algorithmic market participants, Level 3 feeds transmit the complete order book state, sending notifications of every individual active order, its position in the queue, and its specific modifications.
*   **Order Addition:** Broadcasts when a non-matching order rests on the book.
*   **Order Modification:** Notifies on size reductions or changes (e.g., when an iceberg order undergoes replenishment, updating the visible display slice).
*   **Order Execution:** Details fills derived from consumed `[[entities/trade-execution-model]]` packets.

To maintain real-time integrity and mitigate race conditions, L3 update sequence numbers are strictly reconciled with the event logs managed during [[concepts/crash-recovery-durability]] replay scenarios.

---

## Ultra-Low Latency Implementation Patterns

To sustain high-frequency throughput, the Market Data Gateway employs several performance optimizations outlined in [[decisions/configuration-latency-tuning]]:

1.  **Zero-Allocation Mapping:** Incoming text arrays from FIX frames are parsed using custom string-slicing techniques that avoid garbage collector pressure.
2.  **Kernel Bypass Network Cards (Solarflare/onload):** For institutional environments, the gateway runtime is paired with kernel bypass configurations, sending raw TCP/UDP packets directly to user-space memory, circumventing standard operating system network stacks.
3.  **Isolated Partition Threading:** The WebSocket broadcasting engine utilizes individual Go channels per symbol, isolating connection groups so slow client connections in one symbol do not introduce backpressure in unrelated high-velocity markets.