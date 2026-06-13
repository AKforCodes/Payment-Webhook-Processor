package com.payment.webhook.load;

import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.integration.TestcontainersConfig;
import com.payment.webhook.repository.PaymentTransactionRepository;
import org.junit.jupiter.api.Tag;
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
@ContextConfiguration(initializers = TestcontainersConfig.Initializer.class)
@Tag("load")
class LoadTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentTransactionRepository repository;

    @Test
    void shouldHandleConcurrentWebhooksWithoutDataLoss() throws InterruptedException {
        int totalRequests = 100;
        CountDownLatch latch = new CountDownLatch(totalRequests);
        ExecutorService executor = Executors.newFixedThreadPool(50);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        Instant start = Instant.now();

        for (int i = 0; i < totalRequests; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String idempotencyKey = UUID.randomUUID().toString();
                    WebhookEvent event = new WebhookEvent(
                            index % 2 == 0 ? "payment.intent.succeeded" : "payment.intent.failed",
                            new BigDecimal("1000"), "USD",
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

        await().atMost(Duration.ofSeconds(30)).until(() ->
                repository.count() == totalRequests
        );

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
