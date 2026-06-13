# Payment Webhook Processor — MVP Design

## Architecture

Single Spring Boot 3.x application. Maven build. All components in one JAR with clean package separation.

```
com.payment.webhook
├── controller/     REST endpoints
├── service/        Business logic, idempotency
├── kafka/          Producer + consumer
├── entity/         JPA entities
├── repository/     Spring Data JPA
├── dto/            Request/response objects
├── exception/      Error handling
└── config/         Kafka, app config
```

## Data Flow

```
POST /api/webhooks/payment ──→ Controller ──→ Service (idempotency check)
                                                │
                            ├── key exists? → 200 (existing transaction)
                            └── new event  → Kafka producer → "payment-events" topic
                                                                │
                                                           Consumer
                                                                │
                                                         Hibernate → PostgreSQL
```

## Entity: PaymentTransaction

| Field                | Type        | Constraints          |
|----------------------|-------------|----------------------|
| id                   | UUID        | PK                   |
| idempotencyKey       | String      | Unique, NOT NULL     |
| eventType            | String      | NOT NULL             |
| amount               | BigDecimal  | NOT NULL             |
| currency             | String(3)   | NOT NULL             |
| status               | Enum        | PROCESSING/SUCCEEDED/FAILED/REFUNDED/VOIDED |
| providerTransactionId| String      |                      |
| createdAt            | Instant     | NOT NULL             |
| updatedAt            | Instant     | NOT NULL             |

## Idempotency

- Client sends `Idempotency-Key` header on every POST
- Service checks DB unique constraint before Kafka publish
- If key exists → return 200 with existing record (no duplicate charge)
- DB unique constraint on `idempotency_key` guarantees safety on concurrent retries

## REST API

```
POST /api/webhooks/payment
Header: Idempotency-Key: <uuid>
Body: {
  "eventType": "payment.intent.succeeded",
  "amount": 2999,
  "currency": "USD",
  "providerTransactionId": "pi_abc123"
}

Response 200: {
  "id": "uuid-...",
  "status": "PROCESSING",
  "eventType": "payment.intent.succeeded",
  "amount": 2999,
  "currency": "USD",
  "providerTransactionId": "pi_abc123"
}
```

## Error Handling

- Invalid body/fields → 400
- Duplicate idempotency key → 200 with existing record
- Kafka unavailable → 503 with Retry-After
- Consumer failure → 3 retries, then DLQ
- Unknown event type → 202 (accepted, logged)

## Testing

- **Unit:** Controller (MockMvc), Service (Mockito), Kafka producer
- **Integration:** @SpringBootTest + Testcontainers (PostgreSQL + Kafka), end-to-end flow
- **Load:** CountDownLatch test firing 5000 concurrent webhooks, verifying zero message drops
