# Ferroqueue

> A production-grade distributed task queue built in Rust.

Ferroqueue is a distributed message queue designed to explore the systems powering infrastructure like Amazon SQS, Kafka, and RabbitMQ. It implements consensus-based replication, durable storage, automatic retries, dead-letter queues, and production-grade observability.

The project focuses on correctness, fault tolerance, and distributed systems fundamentals rather than raw throughput.

---

## Features

- ⚡ High-performance Rust implementation
- 🗳️ Raft leader election and log replication
- 💾 Durable write-ahead log (WAL)
- 📡 gRPC producer & consumer APIs
- 🔁 At-least-once message delivery
- ⏱️ Visibility timeouts
- ♻️ Automatic retries with configurable backoff
- 📦 Dead Letter Queue (DLQ)
- 🚦 Producer backpressure
- 📈 Prometheus metrics
- 📊 Real-time dashboard
- ☸️ KEDA-compatible autoscaling metrics

---

## Architecture

```text
                 +--------------------+
                 |     Producers      |
                 +---------+----------+
                           |
                        gRPC API
                           |
                    +------v------+
                    | Raft Leader  |
                    +------+-------+
                           |
             Log Replication (Raft)
        +------------------+------------------+
        |                                     |
+-------v-------+                     +-------v-------+
| Follower Node |                     | Follower Node |
+---------------+                     +---------------+

                           |
                  Write-Ahead Log (WAL)
                           |
                  Durable Message Storage
                           |
          +----------------+----------------+
          |                                 |
   Consumer Group A                  Consumer Group B
          |                                 |
     ACK / Retry                     ACK / Retry
          |
 Visibility Timeout
          |
 Dead Letter Queue
```

---

## Delivery Guarantees

### At-Least-Once Delivery

Messages remain in-flight until acknowledged by a consumer.

If the visibility timeout expires before an ACK is received, the broker automatically makes the message available for redelivery.

---

### Durability

Every enqueue operation is appended to a write-ahead log before acknowledging the producer.

On restart, the broker reconstructs queue state from the WAL.

---

### High Availability

Broker nodes participate in a Raft cluster.

- Automatic leader election
- Majority quorum commits
- Log replication
- Split-brain prevention

---

### Retries & Dead Letter Queue

Each message tracks retry attempts.

```
Pending
    ↓
Delivered
    ↓
 ACK? ─────► Complete
    │
    No
    ↓
Visibility Timeout
    ↓
Retry
    ↓
Max Retries?
    │
   Yes
    ↓
Dead Letter Queue
```

---

## Observability

Ferroqueue exports Prometheus metrics including:

- Queue depth
- Consumer lag
- Oldest message age
- Retry count
- Dead-letter count
- Broker health
- Leader status
- Replication latency

These metrics are compatible with Grafana dashboards and KEDA autoscaling.

---

## Planned Components

```
crates/
├── broker/
├── raft/
├── wal/
├── scheduler/
├── storage/
├── grpc/
├── metrics/
├── dashboard/
├── common/
└── cli/
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust |
| RPC | gRPC (tonic) |
| Consensus | Raft |
| Serialization | Protobuf |
| Async Runtime | Tokio |
| Metrics | Prometheus |
| Dashboard | React + Tailwind |
| Storage | Write-Ahead Log |

---

## Roadmap

### Phase 1
- [ ] Single-node queue
- [ ] WAL
- [ ] ACK handling
- [ ] Visibility timeout

### Phase 2
- [ ] Retry scheduler
- [ ] Dead-letter queue
- [ ] Backpressure

### Phase 3
- [ ] Multi-node Raft cluster
- [ ] Log replication
- [ ] Leader election

### Phase 4
- [ ] Prometheus metrics
- [ ] Grafana dashboards
- [ ] Web UI
- [ ] KEDA autoscaling endpoint

---

## Why?

Cloud-native applications rely heavily on distributed queues, yet most engineers interact only with managed services.

Ferroqueue is an educational and production-inspired implementation that explores the mechanisms behind durability, consensus, retries, scheduling, and fault tolerance.

It is designed to answer questions like:

- How does leader election work?
- What happens when a consumer crashes?
- How are retries scheduled?
- How does a queue survive broker failures?
- How can autoscaling decisions be driven by queue depth?

---

## Inspiration

- Amazon SQS
- Apache Kafka
- RabbitMQ
- NATS JetStream
- Raft
- KEDA

---

## License

MIT
