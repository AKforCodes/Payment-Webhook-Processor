package com.payment.webhook.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.webhook.dto.WebhookEvent;
import com.payment.webhook.dto.WebhookResponse;
import com.payment.webhook.entity.TransactionStatus;
import com.payment.webhook.service.WebhookService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
                txId, "payment.intent.succeeded", new BigDecimal("2999"),
                "USD", TransactionStatus.PROCESSING, "pi_abc123", Instant.now()
        );

        when(service.processWebhook(eq("key-123"), any(WebhookEvent.class)))
                .thenReturn(response);

        WebhookEvent event = new WebhookEvent(
                "payment.intent.succeeded", new BigDecimal("2999"), "USD", "pi_abc123"
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
                "payment.intent.succeeded", new BigDecimal("2999"), "USD", "pi_abc123"
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
