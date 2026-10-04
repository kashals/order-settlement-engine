# Order Settlement Engine

An event-driven distributed order and payment settlement system built with Java 21, Spring Boot 3.3.x, Apache Kafka (KRaft mode), and PostgreSQL.

The system demonstrates distributed data consistency across decoupled microservices without distributed locks, addressing dual-write failure modes, duplicate message delivery, and poison-pill recovery.

---

## Architecture Overview

```
[ Client / Load Tester ]
          │
     HTTP POST /orders
          ▼
┌────────────────────────────────────────────────────────┐
│                     ORDER-SERVICE                      │
│                                                        │
│  1. Start local ACID transaction                       │
│  2. INSERT INTO orders (status = PENDING)              │
│  3. INSERT INTO outbox_events (status = PENDING)       │
│  4. Commit DB Transaction                              │
│                                                        │
│  [Scheduled Outbox Poller]                             │
│  5. SELECT * FROM outbox_events                        │
│     WHERE status = 'PENDING'                           │
│     ORDER BY created_at ASC                            │
│     FOR UPDATE SKIP LOCKED                             │
│  6. Publish record to Kafka topic: order.events        │
│  7. UPDATE outbox_events SET status = 'SENT'           │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼ (Kafka Topic: order.events)
┌────────────────────────────────────────────────────────┐
│                    PAYMENT-SERVICE                     │
│                                                        │
│  1. Consume message from order.events                  │
│  2. Atomic deduplication:                              │
│     INSERT INTO processed_events                       │
│     (idempotency_key, consumer_name)                   │
│     VALUES (:key, :name)                               │
│     ON CONFLICT (idempotency_key) DO NOTHING           │
│     ├── 0 rows inserted? Acknowledge & skip            │
│     └── 1 row inserted? Proceed to step 3              │
│  3. Process payment logic & persist to payments table  │
│  4. Publish result event to Kafka: payment.events      │
│     ├── Success (amount <= 10000) -> PaymentCompleted  │
│     └── Failure (amount > 10000)  -> PaymentFailed     │
│  5. Poison Pill Handling:                              │
│     DefaultErrorHandler with 3 retries                 │
│     Exhausted retries route to: order.events.dlq       │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼ (Kafka Topic: payment.events)
┌────────────────────────────────────────────────────────┐
│              ORDER-SERVICE (Saga Closure)              │
│                                                        │
│  [PaymentResultConsumer]                               │
│  1. Consume result from payment.events                 │
│  2. Idempotency guard:                                 │
│     Already CONFIRMED / CANCELLED? Acknowledge & skip  │
│  3. Terminal state update:                             │
│     ├── PaymentCompleted -> UPDATE orders CONFIRMED    │
│     └── PaymentFailed    -> UPDATE orders CANCELLED    │
└────────────────────────────────────────────────────────┘
```

---

## Core Distributed Systems Mechanics

### 1. Dual-Write Elimination (Transactional Outbox Pattern)
Writing state to an operational database and publishing an event to a message broker within the same business action creates a dual-write failure window if the network or broker fails. 
- `order-service` writes both the business entity (`orders`) and the event payload (`outbox_events`) in a single local ACID database transaction.
- A scheduled worker selects pending outbox events using PostgreSQL `FOR UPDATE SKIP LOCKED`. This guarantees zero row contention across concurrent `order-service` replicas while preventing double-publishing.
- Published events transition to `SENT`.

### 2. Consumer Idempotency without Hibernate Rollback Pollution
Kafka guarantees at-least-once message delivery under network partitions and consumer rebalances.
- Deduplication keys are formatted as `orderId:ORDER_CREATED`.
- Inserting into `processed_events` using standard JPA `saveAndFlush()` catches `DataIntegrityViolationException`, but marks the physical Hibernate `EntityManager` session as rollback-only. When Spring attempts to commit the transaction, an `UnexpectedRollbackException` is thrown, erroneously routing valid duplicates to the dead-letter queue.
- To eliminate this defect, deduplication executes via a native PostgreSQL query:
  ```sql
  INSERT INTO processed_events (idempotency_key, consumer_name, processed_at)
  VALUES (:key, :consumerName, CURRENT_TIMESTAMP)
  ON CONFLICT (idempotency_key) DO NOTHING
  ```
  PostgreSQL returns `0` affected rows if the key exists. The consumer acknowledges and discards duplicate events cleanly without transaction corruption.

### 3. Closed Saga Choreography
- `order-service` consumes outcomes from `payment.events` via `PaymentResultConsumer`.
- Orders transition from `PENDING` to terminal states:
  - `PaymentCompletedEvent` transitions orders to `CONFIRMED`.
  - `PaymentFailedEvent` transitions orders to `CANCELLED`.
- Terminal-state validation acts as a second idempotency layer, discarding redundant payment outcome events.

### 4. Poison Pill Routing & Dead Letter Queue (DLQ)
- `payment-service` configures a `DefaultErrorHandler` with a `FixedBackOff` of 1000ms and 3 maximum attempts.
- Unprocessable or malformed payloads trigger retries and are subsequently routed to `order.events.dlq` via Spring Kafka's `DeadLetterPublishingRecoverer`.
- Consumer `enable-auto-commit` is set to `false` with `ack-mode: record` to guarantee that Kafka offsets advance only after successful processing or verified DLQ handover.

### 5. Chunked Outbox Retention Purge
- To prevent unbounded table growth and transaction lock escalation on `outbox_events`, `OutboxPurgeJob` runs a scheduled batch deletion:
  ```sql
  DELETE FROM outbox_events
  WHERE id IN (
      SELECT id FROM outbox_events
      WHERE status = 'SENT' AND created_at < :cutoff
      LIMIT :chunkSize
  )
  ```
- Each chunk executes within an independent transaction via `TransactionTemplate`, preventing table locks and write-ahead log (WAL) spikes on active ingestion workloads.

---

## Project Structure

```
order-settlement-engine/
├── docker-compose.yml                      # Kafka (KRaft), PostgreSQL 16
├── init-dbs.sql                            # Initializes order_db and payment_db
├── pom.xml                                 # Root parent POM
├── common-events/                          # Shared event schema module
│   ├── pom.xml
│   └── src/main/java/com/engine/common/
│       ├── OrderCreatedEvent.java          # Java 21 Record contract
│       ├── PaymentCompletedEvent.java      # Java 21 Record contract
│       └── PaymentFailedEvent.java         # Java 21 Record contract
├── order-service/                          # Order lifecycle & outbox module
│   ├── pom.xml
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── db/migration/
│   │       └── V1__init_order_schema.sql   # Flyway migration
│   └── src/main/java/com/engine/order/
│       ├── OrderServiceApplication.java
│       ├── config/
│       │   └── KafkaTopicConfig.java
│       ├── controller/
│       │   └── OrderController.java
│       ├── consumer/
│       │   └── PaymentResultConsumer.java
│       ├── domain/
│       │   ├── Order.java
│       │   ├── OrderStatus.java
│       │   ├── OutboxEvent.java
│       │   └── OutboxStatus.java
│       ├── dto/
│       │   ├── CreateOrderRequest.java
│       │   └── OrderResponse.java
│       ├── publisher/
│       │   ├── OutboxPublisherJob.java
│       │   └── OutboxPurgeJob.java
│       ├── repository/
│       │   ├── OrderRepository.java
│       │   └── OutboxRepository.java
│       └── service/
│           ├── OrderService.java
│           └── OrderServiceImpl.java
└── payment-service/                        # Payment processing & idempotency module
    ├── pom.xml
    ├── src/main/resources/
    │   ├── application.yml
    │   └── db/migration/
    │       └── V1__init_payment_schema.sql # Flyway migration
    └── src/main/java/com/engine/payment/
        ├── PaymentServiceApplication.java
        ├── config/
        │   ├── KafkaConsumerConfig.java    # ErrorHandler & DLQ recoverer
        │   └── KafkaTopicConfig.java
        ├── consumer/
        │   └── OrderEventConsumer.java     # Atomic deduplication listener
        ├── domain/
        │   ├── Payment.java
        │   ├── PaymentStatus.java
        │   └── ProcessedEvent.java
        ├── repository/
        │   ├── PaymentRepository.java
        │   └── ProcessedEventRepository.java
        └── service/
            └── PaymentProcessor.java
```

---

## Technology Stack

- **Language:** Java 21 (Records, pattern matching, modern switch syntax)
- **Framework:** Spring Boot 3.3.4 (Spring Data JPA, Spring Kafka, Spring Validation)
- **Database:** PostgreSQL 16 (isolated `order_db` and `payment_db` databases)
- **Messaging:** Apache Kafka 7.6.0 (KRaft mode, zero Zookeeper dependencies)
- **Migrations:** Flyway Database Migrations
- **Build Tool:** Apache Maven 3.9.9 (Maven Wrapper included)
- **Infrastructure:** Docker & Docker Compose

---

## Getting Started

### Prerequisites
- Java 21 JDK installed
- Docker and Docker Compose installed and running

### 1. Start Infrastructure
Start PostgreSQL and Kafka in KRaft mode:

```bash
docker compose up -d
```

Verify containers are running:
```bash
docker compose ps
```

### 2. Build Modules
Build and package all modules using the included Maven Wrapper:

```bash
# On Linux/macOS
./mvnw clean install -DskipTests

# On Windows PowerShell
.\mvnw.cmd clean install -DskipTests
```

### 3. Run Microservices
Start both services in separate terminal sessions:

```bash
# Terminal 1: Order Service (Port 8081)
java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar

# Terminal 2: Payment Service (Port 8082)
java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar
```

Flyway executes database schema migrations automatically upon application startup.

---

---

## Verification & curl Examples

### 1. Successful Order Settlement (Amount <= RM 10,000)
Creates an order eligible for payment. The order writes to `orders` and `outbox_events` as `PENDING`, publishes to `order.events`, gets processed by `payment-service` (`SUCCESS`), and transitions to `CONFIRMED`.

```bash
# 1. Create order
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-101","totalAmount":150.50,"currency":"MYR"}'

# 2. Inspect state transition to CONFIRMED (replace with returned order id)
curl http://localhost:8081/api/v1/orders/<ORDER_ID>
```

Expected final order JSON:
```json
{
  "id": "<ORDER_ID>",
  "userId": "user-101",
  "totalAmount": 150.50,
  "currency": "MYR",
  "status": "CONFIRMED"
}
```

### 2. Failed Order Settlement (Amount > RM 10,000)
Simulates a credit limit violation. The payment is rejected by `payment-service` with `CREDIT_LIMIT_EXCEEDED`, publishing `PaymentFailedEvent`, which causes `order-service` to transition the order to `CANCELLED`.

```bash
# 1. Create order exceeding credit limit
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-303","totalAmount":15000.00,"currency":"MYR"}'

# 2. Inspect state transition to CANCELLED
curl http://localhost:8081/api/v1/orders/<ORDER_ID>
```

Expected final order JSON:
```json
{
  "id": "<ORDER_ID>",
  "userId": "user-303",
  "totalAmount": 15000.00,
  "currency": "MYR",
  "status": "CANCELLED"
}
```

---

## Automated Tests

Run the test suite across all modules:

```bash
# Linux/macOS
./mvnw test

# Windows PowerShell
.\mvnw.cmd test
```

### Test Coverage Highlights
- **`OrderEventConsumerTest`** (`payment-service`):
  - Validates successful payment execution on new order events.
  - Verifies atomic deduplication suppresses duplicate events without re-charging.
  - Verifies malformed JSON triggers `IllegalArgumentException` for `DefaultErrorHandler` DLQ routing.
- **`PaymentResultConsumerTest`** (`order-service`):
  - Verifies `PaymentCompletedEvent` transitions orders to `CONFIRMED`.
  - Verifies `PaymentFailedEvent` transitions orders to `CANCELLED`.
  - Verifies duplicate outcome events on terminal states (`CONFIRMED`/`CANCELLED`) are safely discarded.

---

## Benchmark Results

Load testing executed against `order-service` using `hey` (1,000 requests, 20 concurrent workers):

```text
Summary:
  Total:        3.9247 secs
  Slowest:      0.3056 secs
  Fastest:      0.0160 secs
  Average:      0.0772 secs (77.2 ms)
  Requests/sec: 254.7983

Latency Distribution:
  50% in 0.0640 secs (64.0 ms)
  90% in 0.1446 secs (144.6 ms)
  95% in 0.1679 secs (167.9 ms)
  99% in 0.2007 secs (200.7 ms)

Status Code Distribution:
  [201] 1000 responses (100% success rate, 0 errors)
```

### Settlement Verification
Post-benchmark database queries confirmed 100% data integrity:
- `orders`: Exactly 1,003 `CONFIRMED`, 1 `CANCELLED`, 0 `PENDING`.
- `outbox_events`: Exactly 1,004 `SENT`, 0 dropped or hung.
- `processed_events`: Exactly 1,004 recorded keys.
- `payments`: Exactly 1,004 payments recorded with zero duplicate charges.
