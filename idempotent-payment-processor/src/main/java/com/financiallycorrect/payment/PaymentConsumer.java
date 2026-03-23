package com.financiallycorrect.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Component
public class PaymentConsumer {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public PaymentConsumer(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payments-topic", groupId = "payment-processor-group")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        UUID messageId = UUID.fromString(record.key());
        try {
            processIdempotently(messageId, record.value());
            ack.acknowledge();
        } catch (Exception e) {
            System.err.println("Error processing message " + messageId + ": " + e.getMessage());
        }
    }

    @Transactional
    public void processIdempotently(UUID messageId, String payload) throws Exception {
        try {
            // Deduplication via unique constraint on message_id
            jdbcTemplate.update("INSERT INTO processed_messages (message_id) VALUES (?)", messageId);
        } catch (DuplicateKeyException e) {
            System.out.println("Duplicate message completely ignored (idempotent): " + messageId);
            return;
        }

        // Apply business logic since this is the first time seeing this message
        Map<String, Object> data = objectMapper.readValue(payload, Map.class);
        UUID accountId = UUID.fromString((String) data.get("accountId"));
        BigDecimal amount = new BigDecimal(data.get("amount").toString());
        UUID paymentId = UUID.fromString((String) data.get("paymentId"));

        int updated = jdbcTemplate.update("UPDATE target_accounts SET balance = balance + ? WHERE id = ?", amount, accountId);
        if (updated == 0) {
            jdbcTemplate.update("INSERT INTO target_accounts (id, balance) VALUES (?, ?)", accountId, amount);
        }

        // Update upstream payment tracking
        jdbcTemplate.update("UPDATE payments SET status = 'COMPLETED' WHERE id = ?", paymentId);
    }
}
