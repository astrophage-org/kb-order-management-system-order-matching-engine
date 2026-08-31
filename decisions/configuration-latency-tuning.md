# ADR-002: Strategic Configuration and Latency Tuning for Low-Latency Egress

## Status

**Accepted** — October 2023

---

## Context

The Nexus Trading Exchange (NTE) Order Matching Engine (`[[summaries/order-matching-engine]]`) must achieve microsecond-level execution latencies while strictly guaranteeing financial deterministic consistency. It operates as the stateful, memory-centric core of the trading platform.

When executing trades inside isolated `[[entities/order-book]]` instances according to `[[concepts/price-time-priority]]` or handling synthetic liquidity replenishment via the `[[concepts/iceberg-order-engine]]`, matching logic itself takes less than 15 microseconds. However, the system's primary latency bottle-necks arise at the boundary of ingestion and egress:
1.  **Egress Serialization & Network Latency**: Exporting `[[entities/trade-execution-model]]` logs to the `nte.trades.matched` topic and L2 depth updates to the `nte.orderbook.snapshots` topic via the `[[concepts/kafka-integration]]` layer.
2.  **Downstream Durability Guarantees**: Post-trade subsystems, including the `[[entities/trade-settlement-system]]` (TSS) and `[[entities/compliance-surveillance-monitor]]` (CSM), require absolute, zero-loss transaction guarantees. Under high volatility, any lost record or divergence of trade events results in catastrophic settlement and compliance failures. Thus, we must configure `kafka.producer_acks = "all"`.
3.  **Thread Contention & Batching Overhead**: Traditional messaging configurations rely on batching parameters (large `linger.ms` and big buffer allocations) to maximize throughput, but this introduces unacceptably high tail latency (P99/P99.9) for incoming `[[entities/order-model]]` flows.

We require a holistic, strategic latency tuning decision that balances strict crash recovery policies (outlined in `[[concepts/crash-recovery-durability]]`) with continuous, low-jitter real-time routing.

---

## Decision

We will implement a targeted latency-tuning configuration profile optimized for high-throughput, low-jitter trade executions. This pattern is split across three pillars: **Egress Broker Tuning**, **Zero-Batching & Flush Intervals**, and **Multi-Threaded Partition Isolation**.

```
  Inbound Order (Partition N)
            │
            ▼
┌───────────────────────┐
│  `Matching Engine`  │ (Symbol Sharded Registry)
└───────────┬───────────┘
            │
            ▼ (Split-Lock Hand-off via [[decisions/split-locking-pattern]])
┌───────────────────────┐
│    `Order Book`     │ (Independent memory matching)
└───────────┬───────────┘
            │
            ▼ (Zero-Allocation JSON Marshal)
┌─────────────────────────────────────────────────────────────┐
│ Egress Integration Layer: events.KafkaPublisher             │
│                                                             │
│  - "acks": "all"         -> Zero data loss                  │
│  - "linger.ms": 1        -> Microsecond-range flush window   │
│  - "compression": "none" -> CPU latency optimization        │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
           Kafka Brokered Event Streams
           ├── Topic: nte.trades.matched (Keyed by Symbol)
           └── Topic: nte.orderbook.snapshots
```

### 1. Egress Broker & Producer Tuning
To support downstream processing at the `[[entities/trade-settlement-system]]` without introducing blocking overhead inside the `[[entities/matching-engine]]` loop, we configure our `librdkafka` instance to decouple state tracking from I/O thread bottlenecks:
*   **Producer Acknowledgment (`acks = all`)**: Maintained to prevent transactional drift. We mitigate the Round-Trip Time (RTT) penalty of broker replication by employing dedicated 100GbE network fabrics with SR-IOV enabled on NVMe-backed Kafka nodes.
*   **Zero-Batching (`linger.ms = 1` or lower)**: Standard default properties batch messages up to 20-100ms. We restrict this to `1ms` to force near-immediate dispatch to the TCP buffer. This allows minor batching to naturally occur *only* under high-saturation scenarios when the outbound socket buffer is filled.
*   **No Compression (`compression.codec = none`)**: CPU cycles spent compressing small payloads (JSON or binary formats of `[[entities/trade-execution-model]]`) introduce latency spikes. We disable producer-side compression to prioritize raw serialization speeds over bandwidth savings.

### 2. Flush Intervals & Queue Buffers
To prevent queue saturation or lock stalling when matching engines route trades, we establish strict queue tuning limits:
*   **High-Limit Outbound Buffer**: Set `queue.buffering.max.messages = 1000000` to prevent backpressure from propagating into the `[[entities/order-book]]` execution path if Kafka brokers experience transient latency spikes.
*   **Non-Blocking Deliveries**: The Go application delegates payloads to the underlying C-level `librdkafka` threads asynchronously. In the event of a full outbound buffer, the client is configured to drop or log to an engine memory ring buffer rather than blocking the main execution path.

### 3. Multi-Threaded Partition Isolation and Symbol Sharding
To maximize parallelism without sacrificing time priority order, thread isolation is mapped to the structure of the financial assets:
*   **Symbol-Keyed Partitioning**: All events published to Kafka use the `Symbol` string as the routing key. This ensures that all executions for a specific asset (e.g., `BTC/USD`) land in the exact same Kafka partition, guaranteeing deterministic serial delivery for the `[[entities/market-data-gateway]]` and downstream compliance tools.
*   **Thread Allocation Map**: Combining this with the `[[decisions/split-locking-pattern]]`, each active financial instrument's `OrderBook` operates completely isolated within its own virtual thread context. Thread contention is mitigated because we do not share state or locks across different assets.
*   **Worker Thread Pinning**: We pin execution routines for high-volume symbols (e.g., `BTC/USD`, `ETH/USD`) to dedicated CPU cores using OS-level thread affinities (`taskset` or runtime schedulers), ensuring the cache remains hot for matching paths.

---

## Decision Consequences

### Benefits
*   **Minimized Jitter**: Reducing `linger.ms` to `1` keeps P99 dispatch latency under 1.2 milliseconds, ensuring rapid order-book state distribution to the `[[entities/market-data-gateway]]`.
*   **Preserved Integrity**: Maintaining `acks = all` ensures that `[[concepts/crash-recovery-durability]]` routines can reconstruct any system state securely from the Kafka-recorded event log.
*   **High Concurrency**: By leveraging symbol sharding, parallel orders on distinct assets execute simultaneously without global lock boundaries, preventing a backlog in one book from stalling executions in another.

### Trade-offs
*   **Increased Network Overhead**: By minimizing batch sizes, the overall packet-per-second count increases drastically, requiring robust network switches and capable network interfaces to prevent packet drops.
*   **Kafka Broker Resource Consumption**: Dropping batching constraints places higher I/O demands on the Kafka brokers, which must commit transaction segments to physical storage with minimal delay. This requires provisioning high-performance SSDs on the broker layer to prevent disk-write bottlenecks.

## Related Topics
*   `[[concepts/kafka-integration]]` — Implementational details of the low-latency wrapper around `librdkafka`.
*   `[[decisions/split-locking-pattern]]` — Lock structure mapping directly to our parallel symbol-sharded isolation model.
*   `[[concepts/crash-recovery-durability]]` — Rebuilding state from Kafka after system failures.