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
