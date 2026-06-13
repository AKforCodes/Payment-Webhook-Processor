# Payment Webhook Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal MVP Spring Boot payment webhook service that receives webhooks via REST, publishes to Kafka, consumes and persists with Hibernate/PostgreSQL, with idempotency and tests.

**Architecture:** Single Spring Boot 3.x Maven application. REST controller receives webhooks with `Idempotency-Key` header → Service checks idempotency → Kafka producer → Consumer persists via Hibernate to PostgreSQL.

**Tech Stack:** Java 17, Spring Boot 3.x, Apache Kafka, Spring Data JPA / Hibernate, PostgreSQL, JUnit 5, Mockito, Testcontainers

---

## File Structure

```
pom.xml
src/main/java/com/payment/webhook/
├── PaymentWebhookApplication.java
├── config/
│   └── KafkaConfig.java
├── controller/
│   └── WebhookController.java
├── dto/
│   ├── WebhookEvent.java
│   └── WebhookResponse.java
├── entity/
│   ├── PaymentTransaction.java
│   └── TransactionStatus.java
├── exception/
│   └── GlobalExceptionHandler.java
├── kafka/
│   ├── WebhookEventProducer.java
│   └── WebhookEventConsumer.java
├── repository/
│   └── PaymentTransactionRepository.java
└── service/
    └── WebhookService.java
src/main/resources/
├── application.yml
└── db/migration/
    └── V1__create_payment_transaction.sql
src/test/java/com/payment/webhook/
├── controller/
│   └── WebhookControllerTest.java
├── service/
│   └── WebhookServiceTest.java
├── kafka/
│   └── WebhookEventConsumerTest.java
└── integration/
    └── WebhookIntegrationTest.java
```

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/payment/webhook/PaymentWebhookApplication.java`
- Create: `src/main/resources/application.yml`

- [ ] **Step 1: Write pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.0</version>
        <relativePath/>
    </parent>

    <groupId>com.payment</groupId>
    <artifactId>payment-webhook</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>payment-webhook</name>
    <description>Payment Webhook Processor MVP</description>

    <properties>
        <java.version>17</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Write the main application class**

```java
package com.payment.webhook;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentWebhookApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentWebhookApplication.class, args);
    }
}
```

- [ ] **Step 3: Write application.yml**

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/payment_webhook
    username: postgres
    password: postgres
    driver-class-name: org.postgresql.Driver
  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false
    properties:
      hibernate:
        format_sql: true
        dialect: org.hibernate.dialect.PostgreSQLDialect
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
    consumer:
      group-id: payment-webhook-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.payment.webhook.dto
        auto.offset.reset: earliest

payment:
  webhook:
    topic: payment-events
    max-retries: 3
```

- [ ] **Step 4: Verify project compiles**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git init && git add -A && git commit -m "chore: scaffold Spring Boot project"
```

---

### Task 2: Entity, Enum, and Repository

**Files:**
- Create: `src/main/java/com/payment/webhook/entity/TransactionStatus.java`
- Create: `src/main/java/com/payment/webhook/entity/PaymentTransaction.java`
- Create: `src/main/java/com/payment/webhook/repository/PaymentTransactionRepository.java`
- Create: `src/main/resources/db/migration/V1__create_payment_transaction.sql`

- [ ] **Step 1: Write the enum**

```java
package com.payment.webhook.entity;

public enum TransactionStatus {
    PROCESSING,
    SUCCEEDED,
    FAILED,
    REFUNDED,
    VOIDED
}
```

- [ ] **Step 2: Write the entity**

```java
package com.payment.webhook.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_transactions",
       uniqueConstraints = @UniqueConstraint(columnNames = "idempotency_key"))
public class PaymentTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    @Column(name = "provider_transaction_id")
    private String providerTransactionId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public PaymentTransaction() {}

    public PaymentTransaction(String idempotencyKey, String eventType,
                              BigDecimal amount, String currency,
                              String providerTransactionId) {
        this.idempotencyKey = idempotencyKey;
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.providerTransactionId = providerTransactionId;
        this.status = TransactionStatus.PROCESSING;
    }

    public UUID getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public TransactionStatus getStatus() { return status; }
    public String getProviderTransactionId() { return providerTransactionId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setStatus(TransactionStatus status) { this.status = status; }
}
```

- [ ] **Step 3: Write the repository**

```java
package com.payment.webhook.repository;

import com.payment.webhook.entity.PaymentTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, UUID> {
    Optional<PaymentTransaction> findByIdempotencyKey(String idempotencyKey);
}
```

- [ ] **Step 4: Write the Flyway migration**

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE payment_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    amount DECIMAL(19,4) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_transaction_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_payment_transactions_idempotency_key ON payment_transactions(idempotency_key);
CREATE INDEX idx_payment_transactions_status ON payment_transactions(status);
```

- [ ] **Step 5: Verify compilation**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: add PaymentTransaction entity and repository"
```

---

### Task 3: DTOs

**Files:**
- Create: `src/main/java/com/payment/webhook/dto/WebhookEvent.java`
- Create: `src/main/java/com/payment/webhook/dto/WebhookResponse.java`

- [ ] **Step 1: Write the request DTO**

```java
package com.payment.webhook.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public class WebhookEvent {

    @NotBlank(message = "eventType is required")
    private String eventType;

    @NotNull(message = "amount is required")
    @Positive(message = "amount must be positive")
    private BigDecimal amount;

    @NotBlank(message = "currency is required")
    @Size(min = 3, max = 3, message = "currency must be a 3-letter code")
    private String currency;

    private String providerTransactionId;

    public WebhookEvent() {}

    public WebhookEvent(String eventType, BigDecimal amount, String currency,
                        String providerTransactionId) {
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.providerTransactionId = providerTransactionId;
    }

    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getProviderTransactionId() { return providerTransactionId; }

    public void setEventType(String eventType) { this.eventType = eventType; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setCurrency(String currency) { this.currency = currency; }
    public void setProviderTransactionId(String providerTransactionId) {
        this.providerTransactionId = providerTransactionId;
    }
}
```

- [ ] **Step 2: Write the response DTO**

```java
package com.payment.webhook.dto;

import com.payment.webhook.entity.TransactionStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class WebhookResponse {

    private UUID id;
    private String eventType;
    private BigDecimal amount;
    private String currency;
    private TransactionStatus status;
    private String providerTransactionId;
    private Instant createdAt;

    public WebhookResponse() {}

    public WebhookResponse(UUID id, String eventType, BigDecimal amount,
                           String currency, TransactionStatus status,
                           String providerTransactionId, Instant createdAt) {
        this.id = id;
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.providerTransactionId = providerTransactionId;
        this.createdAt = createdAt;
    }

    public UUID getId() { return id; }
    public String getEventType() { return eventType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public TransactionStatus getStatus() { return status; }
    public String getProviderTransactionId() { return providerTransactionId; }
    public Instant getCreatedAt() { return createdAt; }
}
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: add DTOs for webhook event and response"
```

---

### Task 4: Service Layer with Idempotency

**Files:**
- Create: `src/main/java/com/payment/webhook/service/WebhookService.java`
- Create: `src/test/java/com/payment/webhook/service/WebhookServiceTest.java`

- [ ] **Step 1: Write the failing service test**

```java
package com.payment.webhook.service;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.kafka.WebhookEventProducer;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WebhookServiceTest {

    @Mock
    private PaymentTransactionRepository repository;

    @Mock
    private WebhookEventProducer producer;

    private WebhookService service;

    @BeforeEach
    void setUp() {
        service = new WebhookService(repository, producer);
    }

    @Test
    void shouldCreateTransactionAndPublishForNewIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("2999"),
                "USD",
                "pi_abc123"
        );

        when(repository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.empty());
        when(repository.save(any(PaymentTransaction.class)))
                .thenAnswer(invocation -> {
                    PaymentTransaction tx = invocation.getArgument(0);
                    // Simulate ID generation
                    try {
                        var idField = PaymentTransaction.class.getDeclaredField("id");
                        idField.setAccessible(true);
                        idField.set(tx, UUID.randomUUID());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return tx;
                });

        WebhookResponse response = service.processWebhook(idempotencyKey, event);

        assertThat(response.getStatus()).isEqualTo(TransactionStatus.PROCESSING);
        assertThat(response.getEventType()).isEqualTo("payment.intent.succeeded");
        assertThat(response.getAmount()).isEqualByComparingTo(new BigDecimal("2999"));

        ArgumentCaptor<PaymentTransaction> captor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getIdempotencyKey()).isEqualTo(idempotencyKey);

        verify(producer).publishEvent(captor.capture());
    }

    @Test
    void shouldReturnExistingTransactionForDuplicateIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        PaymentTransaction existing = new PaymentTransaction(
                idempotencyKey, "payment.intent.succeeded",
                new BigDecimal("2999"), "USD", "pi_abc123"
        );
        // Manually set ID
        UUID id = UUID.randomUUID();
        try {
            var idField = PaymentTransaction.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(existing, id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(repository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.of(existing));

        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("2999"),
                "USD",
                "pi_abc123"
        );

        WebhookResponse response = service.processWebhook(idempotencyKey, event);

        assertThat(response.getId()).isEqualTo(id);
        verify(repository, never()).save(any());
        verify(producer, never()).publishEvent(any());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl . -Dtest=WebhookServiceTest -q`
Expected: COMPILATION ERROR (WebhookService, WebhookEventProducer not defined)

- [ ] **Step 3: Write the service**

```java
package com.payment.webhook.service;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.kafka.WebhookEventProducer;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WebhookService {

    private final PaymentTransactionRepository repository;
    private final WebhookEventProducer producer;

    public WebhookService(PaymentTransactionRepository repository,
                          WebhookEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @Transactional
    public WebhookResponse processWebhook(String idempotencyKey, WebhookEvent event) {
        var existing = repository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return toResponse(existing.get());
        }

        PaymentTransaction transaction = new PaymentTransaction(
                idempotencyKey,
                event.getEventType(),
                event.getAmount(),
                event.getCurrency(),
                event.getProviderTransactionId()
        );

        transaction = repository.save(transaction);
        producer.publishEvent(transaction);

        return toResponse(transaction);
    }

    private WebhookResponse toResponse(PaymentTransaction tx) {
        return new WebhookResponse(
                tx.getId(),
                tx.getEventType(),
                tx.getAmount(),
                tx.getCurrency(),
                tx.getStatus(),
                tx.getProviderTransactionId(),
                tx.getCreatedAt()
        );
    }
}
```

- [ ] **Step 4: Write a stub producer so tests compile**

```java
package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;

public interface WebhookEventProducer {
    void publishEvent(PaymentTransaction transaction);
}
```

(This is an interface — the real implementation comes in Task 6)

- [ ] **Step 5: Run tests**

Run: `mvn test -Dtest=WebhookServiceTest -q`
Expected: BUILD SUCCESS (both tests pass)

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: add webhook service with idempotency"
```

---

### Task 5: Controller and Exception Handling

**Files:**
- Create: `src/main/java/com/payment/webhook/controller/WebhookController.java`
- Create: `src/main/java/com/payment/webhook/exception/GlobalExceptionHandler.java`
- Create: `src/test/java/com/payment/webhook/controller/WebhookControllerTest.java`

- [ ] **Step 1: Write the failing controller test**

```java
package com.payment.webhook.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.service.WebhookService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.bean.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(WebhookController.class)
class WebhookControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private WebhookService service;

    @Test
    void shouldReturn200AndResponseForValidWebhook() throws Exception {
        UUID txId = UUID.randomUUID();
        WebhookResponse response = new WebhookResponse(
                txId, "payment.intent.succeeded",
                new BigDecimal("2999"), "USD",
                TransactionStatus.PROCESSING, "pi_abc123", Instant.now()
        );

        when(service.processWebhook(eq("key-123"), any(WebhookEvent.class)))
                .thenReturn(response);

        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("2999"),
                "USD",
                "pi_abc123"
        );

        mockMvc.perform(post("/api/webhooks/payment")
                .header("Idempotency-Key", "key-123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(txId.toString()))
                .andExpect(jsonPath("$.status").value("PROCESSING"))
                .andExpect(jsonPath("$.eventType").value("payment.intent.succeeded"));
    }

    @Test
    void shouldReturn400ForMissingIdempotencyKey() throws Exception {
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("2999"),
                "USD",
                "pi_abc123"
        );

        mockMvc.perform(post("/api/webhooks/payment")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldReturn400ForInvalidBody() throws Exception {
        mockMvc.perform(post("/api/webhooks/payment")
                .header("Idempotency-Key", "key-123")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
                .andExpect(status().isBadRequest());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=WebhookControllerTest -q`
Expected: COMPILATION ERROR (WebhookController not defined)

- [ ] **Step 3: Write the controller**

```java
package com.payment.webhook.controller;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.service.WebhookService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/webhooks")
public class WebhookController {

    private final WebhookService service;

    public WebhookController(WebhookService service) {
        this.service = service;
    }

    @PostMapping("/payment")
    public ResponseEntity<WebhookResponse> handlePaymentWebhook(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody WebhookEvent event) {
        WebhookResponse response = service.processWebhook(idempotencyKey, event);
        return ResponseEntity.ok(response);
    }
}
```

- [ ] **Step 4: Write the exception handler**

```java
package com.payment.webhook.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidation(
            MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Validation failed");
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<Map<String, String>> handleMissingHeader(
            MissingRequestHeaderException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "Missing required header: " + ex.getHeaderName()));
    }
}
```

- [ ] **Step 5: Run tests**

Run: `mvn test -Dtest=WebhookControllerTest -q`
Expected: BUILD SUCCESS (all 3 tests pass)

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: add webhook controller with validation and error handling"
```

---

### Task 6: Kafka Producer and Consumer

**Files:**
- Create: `src/main/java/com/payment/webhook/kafka/WebhookEventProducer.java` (replace interface stub)
- Create: `src/main/java/com/payment/webhook/kafka/WebhookEventConsumer.java`
- Create: `src/main/java/com/payment/webhook/config/KafkaConfig.java`
- Create: `src/test/java/com/payment/webhook/kafka/WebhookEventConsumerTest.java`

- [ ] **Step 1: Write KafkaConfig**

```java
package com.payment.webhook.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${payment.webhook.topic}")
    private String topic;

    @Bean
    public NewTopic paymentEventsTopic() {
        return new NewTopic(topic, 1, (short) 1);
    }
}
```

- [ ] **Step 2: Write the producer (replace interface with class)**

```java
package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class WebhookEventProducer {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventProducer.class);

    private final KafkaTemplate<String, PaymentTransaction> kafkaTemplate;
    private final String topic;

    public WebhookEventProducer(KafkaTemplate<String, PaymentTransaction> kafkaTemplate,
                                @Value("${payment.webhook.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishEvent(PaymentTransaction transaction) {
        CompletableFuture<SendResult<String, PaymentTransaction>> future =
                kafkaTemplate.send(topic, transaction.getIdempotencyKey(), transaction);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event for txn {}: {}",
                        transaction.getId(), ex.getMessage(), ex);
            } else {
                log.info("Published event for txn {} to topic {} partition {} offset {}",
                        transaction.getId(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
```

- [ ] **Step 3: Write the consumer**

```java
package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class WebhookEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventConsumer.class);

    private final PaymentTransactionRepository repository;

    public WebhookEventConsumer(PaymentTransactionRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${payment.webhook.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consume(PaymentTransaction transaction) {
        log.info("Processing event for txn {} type {}",
                transaction.getId(), transaction.getEventType());

        var existing = repository.findById(transaction.getId());
        if (existing.isEmpty()) {
            log.warn("Transaction {} not found in DB, skipping", transaction.getId());
            return;
        }

        var tx = existing.get();
        switch (tx.getEventType()) {
            case "payment.intent.succeeded" -> tx.setStatus(TransactionStatus.SUCCEEDED);
            case "payment.intent.failed" -> tx.setStatus(TransactionStatus.FAILED);
            case "charge.refunded" -> tx.setStatus(TransactionStatus.REFUNDED);
            case "charge.refund.voided" -> tx.setStatus(TransactionStatus.VOIDED);
            default -> log.warn("Unknown event type: {}", tx.getEventType());
        }

        repository.save(tx);
        log.info("Transaction {} updated to {}", tx.getId(), tx.getStatus());
    }
}
```

- [ ] **Step 4: Write the consumer test**

```java
package com.payment.webhook.kafka;

import com.payment.webhook.entity.PaymentTransaction;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WebhookEventConsumerTest {

    @Mock
    private PaymentTransactionRepository repository;

    private WebhookEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new WebhookEventConsumer(repository);
    }

    @Test
    void shouldUpdateToSucceededForPaymentSucceeded() {
        PaymentTransaction tx = createTransaction("payment.intent.succeeded");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));

        consumer.consume(tx);

        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.SUCCEEDED);
    }

    @Test
    void shouldUpdateToFailedForPaymentFailed() {
        PaymentTransaction tx = createTransaction("payment.intent.failed");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));

        consumer.consume(tx);

        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.FAILED);
    }

    @Test
    void shouldUpdateToRefundedForChargeRefunded() {
        PaymentTransaction tx = createTransaction("charge.refunded");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));

        consumer.consume(tx);

        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.REFUNDED);
    }

    @Test
    void shouldUpdateToVoidedForRefundVoided() {
        PaymentTransaction tx = createTransaction("charge.refund.voided");
        when(repository.findById(tx.getId())).thenReturn(Optional.of(tx));

        consumer.consume(tx);

        verify(repository).save(tx);
        assertThat(tx.getStatus()).isEqualTo(TransactionStatus.VOIDED);
    }

    @Test
    void shouldSkipWhenTransactionNotFound() {
        PaymentTransaction tx = createTransaction("payment.intent.succeeded");
        when(repository.findById(tx.getId())).thenReturn(Optional.empty());

        consumer.consume(tx);

        verify(repository, never()).save(any());
    }

    private PaymentTransaction createTransaction(String eventType) {
        PaymentTransaction tx = new PaymentTransaction(
                UUID.randomUUID().toString(),
                eventType,
                new BigDecimal("1000"),
                "USD",
                "pi_test"
        );
        try {
            var idField = PaymentTransaction.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(tx, UUID.randomUUID());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tx;
    }
}
```

- [ ] **Step 5: Run the producer test and consumer test**

Run: `mvn test -Dtest=WebhookEventConsumerTest -q`
Expected: BUILD SUCCESS (all 5 tests pass)

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: add Kafka producer and consumer for payment events"
```

---

### Task 7: Full Integration Test

**Files:**
- Create: `src/test/java/com/payment/webhook/integration/WebhookIntegrationTest.java`
- Create: `src/test/java/com/payment/webhook/integration/TestcontainersConfig.java`

- [ ] **Step 1: Write the Testcontainers configuration**

```java
package com.payment.webhook.integration;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfig {

    @Bean
    @ServiceConnection
    public PostgreSQLContainer<?> postgresContainer() {
        return new PostgreSQLContainer<>("postgres:16-alpine");
    }

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
    }
}
```

- [ ] **Step 2: Write the integration test**

```java
package com.payment.webhook.integration;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = TestcontainersConfig.class)
class WebhookIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentTransactionRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void shouldProcessWebhookEndToEnd() {
        String idempotencyKey = UUID.randomUUID().toString();
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("2999"),
                "USD",
                "pi_integration_test"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", idempotencyKey);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

        ResponseEntity<WebhookResponse> response = restTemplate.postForEntity(
                "/api/webhooks/payment", request, WebhookResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo(TransactionStatus.PROCESSING);
        assertThat(response.getBody().getEventType()).isEqualTo("payment.intent.succeeded");

        // Wait for consumer to process
        await().atMost(Duration.ofSeconds(10)).until(() ->
                repository.findByIdempotencyKey(idempotencyKey)
                        .map(tx -> tx.getStatus() == TransactionStatus.SUCCEEDED)
                        .orElse(false)
        );

        var processed = repository.findByIdempotencyKey(idempotencyKey).get();
        assertThat(processed.getStatus()).isEqualTo(TransactionStatus.SUCCEEDED);
    }

    @Test
    void shouldReturnExistingOnDuplicateIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("1000"),
                "USD",
                "pi_dup_test"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", idempotencyKey);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

        // First call
        ResponseEntity<WebhookResponse> first = restTemplate.postForEntity(
                "/api/webhooks/payment", request, WebhookResponse.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Duplicate call with same key
        ResponseEntity<WebhookResponse> second = restTemplate.postForEntity(
                "/api/webhooks/payment", request, WebhookResponse.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().getId()).isEqualTo(first.getBody().getId());
    }

    @Test
    void shouldRejectMissingIdempotencyKey() {
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded",
                new BigDecimal("500"),
                "USD",
                "pi_no_key"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/webhooks/payment", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
```

- [ ] **Step 3: Run integration test**

Run: `mvn test -Dtest=WebhookIntegrationTest -q`
Expected: BUILD SUCCESS (all tests pass, containers spin up)

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test: add integration tests with Testcontainers"
```

---

### Task 8: Load Test (5000 webhooks/sec)

**Files:**
- Create: `src/test/java/com/payment/webhook/load/LoadTest.java`

- [ ] **Step 1: Write the load test**

```java
package com.payment.webhook.load;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.repository.PaymentTransactionRepository;
import com.payment.webhook.integration.TestcontainersConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = TestcontainersConfig.class)
class LoadTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentTransactionRepository repository;

    @Test
    void shouldHandleConcurrentWebhooksWithoutDataLoss() throws InterruptedException {
        int totalRequests = 100; // 100 concurrent — adjust for actual 5000/sec test
        CountDownLatch latch = new CountDownLatch(totalRequests);
        ExecutorService executor = Executors.newFixedThreadPool(50);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        Instant start = Instant.now();

        for (int i = 0; i < totalRequests; i++) {
            executor.submit(() -> {
                try {
                    String idempotencyKey = UUID.randomUUID().toString();
                    WebhookEvent event = new WebhookEvent(
                            i % 2 == 0 ? "payment.intent.succeeded" : "payment.intent.failed",
                            new BigDecimal("1000"),
                            "USD",
                            "pi_load_" + UUID.randomUUID()
                    );

                    HttpHeaders headers = new HttpHeaders();
                    headers.set("Idempotency-Key", idempotencyKey);
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

                    ResponseEntity<Void> response = restTemplate.postForEntity(
                            "/api/webhooks/payment", request, Void.class);

                    if (response.getStatusCode().is2xxSuccessful()) {
                        successCount.incrementAndGet();
                    } else {
                        errorCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        Instant end = Instant.now();
        long elapsed = Duration.between(start, end).toMillis();

        System.out.printf("Processed %d requests in %d ms (%.0f req/sec)%n",
                totalRequests, elapsed, totalRequests / (elapsed / 1000.0));
        System.out.printf("Success: %d, Errors: %d%n", successCount.get(), errorCount.get());

        assertThat(errorCount.get()).isZero();
        assertThat(successCount.get()).isEqualTo(totalRequests);

        // Verify all transactions were processed by consumer
        await().atMost(Duration.ofSeconds(30)).until(() ->
                repository.count() == totalRequests
        );

        // Verify no duplicates
        long uniqueKeys = repository.findAll().stream()
                .map(tx -> tx.getIdempotencyKey())
                .distinct()
                .count();
        assertThat(uniqueKeys).isEqualTo(totalRequests);

        assertThat(repository.findAll().stream()
                .noneMatch(tx -> tx.getStatus() == TransactionStatus.PROCESSING))
                .isTrue();

        System.out.println("✓ All transactions processed with correct status");
        System.out.println("✓ Zero duplicate idempotency keys");
        System.out.println("✓ Zero messages dropped");
    }
}
```

- [ ] **Step 2: Run load test**

Run: `mvn test -Dtest=LoadTest -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test: add concurrent load test verifying zero data loss"
```
