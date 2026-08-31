# Crash Recovery & Durability Strategy

The Nexus Trading Exchange (NTE) Order Matching Engine (OME) maintains its entire active state—including limit order books, priority queues, and trading registries—strictly in-memory. This state-to-memory localization ensures execution latencies remain in the microsecond range. 

To satisfy the requirements of financial auditability, crash resilience, and strict transaction durability without sacrificing performance, the OME utilizes an **Event Sourcing** pattern. The immutable source of truth is a sequential write-ahead log backed by Apache Kafka, supplemented by deterministic, state-compressed **End-of-Day (EOD) snapshots**.

---

## 1. The Durability Architecture

The system models its state mutations as a deterministic state machine. For any given starting state $S_0$ and a sequence of inbound events $E_{1..n}$, the resulting state $S_n$ is always identical:

$$S_n = f(S_0, E_{1..n})$$

Because database disk writes are too slow to run inline with the core matching loop, the OME shifts the durability boundary to the messaging layer.

```
                                      CRASH EVENT
                                           │
                                           ▼ (System Reboots)
┌──────────────────┐               ┌───────────────┐               ┌────────────────┐
│  S3/GCS Storage  │               │ Inbound Kafka │               │  In-Memory     │
│  (EOD Snapshot)  │               │   Partition   │               │  `Order Book` │
└─────────┬────────┘               └───────┬───────┘               └───────┬────────┘
          │                                │                               │
          │ 1. Load Snapshot (S_0)         │                               │
          ├───────────────────────────────────────────────────────────────>│
          │                                │                               │ (State at S_0)
          │                                │ 2. Replay remaining logs      │
          │                                │    from checkpoint offset     │
          │                                ├──────────────────────────────>│
          │                                │                               │ (Deterministic Replay)
          │                                │                               │
          │                                │                               ▼
                                                                     [Fully Restored]
                                                                     (Ready for Traffic)
```

1. **Inbound Commands:** All client actions (Limit, Market, and Iceberg orders) are received by the [[entities/market-data-gateway]] and published to the `nte.orders.inbound` Kafka topic.
2. **In-Memory Core:** The [[entities/matching-engine]] consumes this topic, applying mutations inside isolated [[entities/order-book]] structures.
3. **Outbound Reports:** Output transactions (the [[entities/trade-execution-model]] instances) are published to the `nte.trades.matched` topic.

In the event of a process crash or hardware failure, the memory space is lost. However, the exact state is reconstructed by reading the most recent EOD snapshot and replaying the subsequent inbound events sequentially.

---

## 2. Event Sourcing Replay Strategy

Upon startup, the matching engine determines whether it is booting clean (e.g., at the start of a trading day) or recovering from an unscheduled crash. 

### Parallel Replay via Symbol Partitioning
As defined in [[concepts/kafka-integration]], Kafka topics are partitioned deterministically by financial `Symbol` (e.g., `BTC/USD`). This keying strategy ensures that:
* Every order for a given symbol is written to the exact same Kafka partition.
* Chronological order-priority is strictly preserved within that partition.
* Recovery can occur in parallel: different symbol-specific [[entities/order-book]] instances can be rebuilt concurrently on separate threads.

### The Recovery Control Loop
During boot, the OME follows this orchestration sequence:

```go
// Simplified representation of the recovery orchestrator in the Matching Engine
func (e *Engine) RecoverState(snapshotTime time.Time, lastProcessedOffset int64) error {
    e.log.Info("Starting recovery sequence...")
    
    // 1. Load the nearest base snapshot
    snapshot, err := loadSnapshotFromStorage(snapshotTime)
    if err != nil {
        return fmt.Errorf("failed to load base snapshot: %w", err)
    }
    
    // 2. Hydrate the local symbol registries
    for symbol, bookState := range snapshot.Books {
        e.books[symbol] = matching.RestoreOrderBook(symbol, bookState)
    }
    
    // 3. Initialize Kafka consumer at the target checkpoint offset
    consumer, err := events.NewRecoveryConsumer(e.kafkaBrokers, "nte.orders.inbound", lastProcessedOffset)
    if err != nil {
        return fmt.Errorf("failed to init recovery consumer: %w", err)
    }
    defer consumer.Close()

    // 4. Enter Replay Loop (Silent Recovery Mode)
    e.log.Info("Replaying historical transaction log...")
    for {
        msg, EOF, err := consumer.PollNextMessage()
        if err != nil {
            return err
        }
        if EOF {
            break // Caught up to the log head
        }

        var order models.Order
        if err := json.Unmarshal(msg.Value, &order); err != nil {
            continue
        }

        // Execute match logic WITHOUT publishing output events to egress topics
        _, _ = e.books[order.Symbol].AddOrderSilent(order)
    }

    e.log.Info("State synchronization complete. Handing over to active matching loop.")
    return nil
}
```

### The "Silent Mode" Execution Guard
A critical hazard of event sourcing replay is the potential for **duplicate output publishing**. If a recovering engine processes an old order and publishes a matching trade to `nte.trades.matched`, downstream systems—such as the [[entities/trade-settlement-system]] and the [[entities/compliance-surveillance-monitor]]—will receive duplicate trades, causing margin errors, double-clearing, or false wash-trading alerts.

To prevent this, the [[entities/order-book]] implements two processing modes:
1. **Active Mode (`AddOrder`):** Processes the order, modifies the priority queues, replenishes [[concepts/iceberg-order-engine]] display slices, and returns executed trades to be broadcast to Kafka.
2. **Silent Mode (`AddOrderSilent`):** Executes identical internal mutations (matching, FIFO priority queuing, time-priority tracking) but explicitly suppresses outbound network triggers and metric updates.

---

## 3. End-of-Day (EOD) Snapshot Generation

To prevent the Kafka replay window from growing indefinitely—which would dramatically increase the system's Recovery Time Objective (RTO)—the platform enforces an End-of-Day snapshot cycle based on the `operating_hours` configured in `config/exchange.yaml`.

At the closing time (e.g., `16:00:00`), the engine transitions from active matching to a snapshot generation phase.

### Snapshot Generation Steps
1. **Drain Inbound Queue:** The engine stops consuming new user-submitted orders. It finishes processing all outstanding messages in the inbound partition buffer to reach a stable, quiescent state.
2. **Acquire Read Locks:** The engine briefly acquires a global read-lock (`sync.RWMutex.RLock`) to pause mutations across all symbol books. This uses the [[decisions/split-locking-pattern]] to safely read-lock individual books.
3. **Serialization:** Each active [[entities/order-book]] translates its current internal state—including bids, asks, execution queues, and individual [[entities/order-model]] parameters—into a compressed JSON or Protocol Buffers snapshot format.
4. **Persist and Checkpoint:** The serialized snapshot payload is pushed asynchronously to cloud storage (S3/GCS). The metadata, including the corresponding Kafka topic partition offset, is saved.
5. **Clear Book Memory:** Once verified, the active books are purged or marked as archived. The next trading session begins with a clean state hydrate step.

### Snapshot Schema Structure
```json
{
  "snapshot_id": "snap-nte-prod-01-20231024-160000",
  "exchange_id": "NTE-PROD-01",
  "timestamp": "2023-10-24T16:00:00Z",
  "kafka_offsets": {
    "nte.orders.inbound": {
      "partition_0": 9821445,
      "partition_1": 9821490,
      "partition_2": 9821381
    }
  },
  "books": {
    "BTC/USD": {
      "bids": [
        { "id": "ord-883", "trader_id": "TR_A", "type": "LIMIT", "price": 65430.00, "qty": 1.5, "timestamp": 1698163189000 },
        { "id": "ord-890", "trader_id": "TR_B", "type": "LIMIT", "price": 65428.50, "qty": 4.0, "is_iceberg": true, "display_qty": 1.0, "hidden_qty": 3.0 }
      ],
      "asks": []
    }
  }
}
```

---

## 4. Tuning for Low-Latency Durability

To guarantee that no transaction is lost between the memory mutation and the physical log layer, the Kafka producer configuration must be tuned aggressively, balancing write throughput against data safety:

| Configuration Property | Value | Architectural Impact |
| :--- | :--- | :--- |
| `producer_acks` | `"all"` | Ensures that an order is acknowledged only after being written to the lead broker and all synchronized replicas (ISR). Prevents split-brain state scenarios. |
| `linger.ms` | `1` | Forces immediate flushing of order matches. While zero-batching maximizes speed, a `1ms` delay allows minor micro-batching under load without impacting recovery latency. See [[decisions/configuration-latency-tuning]]. |
| `compression.type` | `"none"` | Avoids processing overhead and CPU cycles for compression, reducing microsecond-level outbound latency. |

Through this combination of Kafka write-ahead logs, symbol-partitioned recovery, silent-mode execution, and daily snapshots, the NTE Order Matching Engine achieves a highly resilient **Recovery Point Objective (RPO) of zero** and a **Recovery Time Objective (RTO) of under 2 minutes** for multi-gigabyte order books.