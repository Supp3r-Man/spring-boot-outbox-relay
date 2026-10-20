package com.github.sup3rmar10.outbox.web;

import com.github.sup3rmar10.outbox.payment.Payment;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    public record CreatePayment(
            @NotBlank @Size(max = 64) String accountId,
            @Positive long amountMinor,
            @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {
    }

    private final PaymentService payments;

    public PaymentController(PaymentService payments) {
        this.payments = payments;
    }

    @PostMapping("/payments")
    ResponseEntity<Payment> create(@Valid @RequestBody CreatePayment body) {
        Payment p = payments.create(body.accountId(), body.amountMinor(), body.currency());
        return ResponseEntity.created(URI.create("/payments/" + p.id())).body(p);
    }

    @GetMapping("/payments/{id}")
    Payment get(@PathVariable String id) {
        return payments.get(id);
    }

    @PostMapping("/payments/{id}/capture")
    Payment capture(@PathVariable String id) {
        return payments.capture(id);
    }

    @PostMapping("/payments/{id}/refund")
    Payment refund(@PathVariable String id) {
        return payments.refund(id);
    }
}
