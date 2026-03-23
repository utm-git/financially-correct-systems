package com.financiallycorrect.payment;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class OutboxRelay {
    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void relayUnpublishedMessages() {
        String sql = "SELECT id, message_id, payload FROM payment_outbox WHERE published = false ORDER BY id ASC LIMIT 50";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

        for (Map<String, Object> row : rows) {
            Long id = ((Number) row.get("id")).longValue();
            UUID messageId = (UUID) row.get("message_id");
            String payload = row.get("payload").toString();

            try {
                // Synchronous send to ensure at-least-once before marking as published
                kafkaTemplate.send("payments-topic", messageId.toString(), payload).get();
                
                // Mark as published
                jdbcTemplate.update("UPDATE payment_outbox SET published = true WHERE id = ?", id);
            } catch (Exception e) {
                // Stop processing batch if Kafka is down or publishing fails
                System.err.println("Failed to relay outbox message: " + e.getMessage());
                break; 
            }
        }
    }
}
