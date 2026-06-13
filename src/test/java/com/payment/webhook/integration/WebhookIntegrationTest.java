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
@ContextConfiguration(initializers = TestcontainersConfig.Initializer.class)
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
                "payment.intent.succeeded", new BigDecimal("2999"), "USD", "pi_integration_test"
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
                "payment.intent.succeeded", new BigDecimal("1000"), "USD", "pi_dup_test"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", idempotencyKey);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

        ResponseEntity<WebhookResponse> first = restTemplate.postForEntity(
                "/api/webhooks/payment", request, WebhookResponse.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<WebhookResponse> second = restTemplate.postForEntity(
                "/api/webhooks/payment", request, WebhookResponse.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().getId()).isEqualTo(first.getBody().getId());
    }

    @Test
    void shouldRejectMissingIdempotencyKey() {
        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded", new BigDecimal("500"), "USD", "pi_no_key"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<WebhookEvent> request = new HttpEntity<>(event, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/webhooks/payment", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
