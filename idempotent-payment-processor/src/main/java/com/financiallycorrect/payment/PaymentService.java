package com.financiallycorrect.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Service
public class PaymentService {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public PaymentService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public UUID initiatePayment(UUID targetAccountId, BigDecimal amount) {
        UUID paymentId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO payments (id, amount, status) VALUES (?, ?, ?)", paymentId, amount, "INIT");

        UUID messageId = UUID.randomUUID();
        String payload;
        try {
            payload = objectMapper.writeValueAsString(Map.of(
                    "paymentId", paymentId,
                    "accountId", targetAccountId,
                    "amount", amount
            ));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }

        // Insert event into outbox in the same transaction
        jdbcTemplate.update("INSERT INTO payment_outbox (message_id, payment_id, payload) VALUES (?, ?, ?::jsonb)", 
                messageId, paymentId, payload);
                
        return paymentId;
    }
}
