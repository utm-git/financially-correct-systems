package com.financiallycorrect.payment;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Testcontainers
class PaymentIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("test_payment")
            .withUsername("test")
            .withPassword("test");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentConsumer paymentConsumer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testOutboxAndIdempotency() throws Exception {
        UUID targetAccount = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100.00");

        // 1. Initiate payment (Writes to payments + outbox)
        paymentService.initiatePayment(targetAccount, amount);

        // 2. Wait for Relay to push to Kafka and Consumer to process
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            Integer count = jdbcTemplate.queryForObject("SELECT count(*) FROM processed_messages", Integer.class);
            return count != null && count > 0;
        });

        // 3. Verify exactly 100 was credited
        BigDecimal balance = jdbcTemplate.queryForObject(
                "SELECT balance FROM target_accounts WHERE id = ?", BigDecimal.class, targetAccount);
        assertEquals(0, new BigDecimal("100.00").compareTo(balance), "Balance should be exactly 100.00");

        // 4. Force duplicate message to test idempotency
        String messageIdStr = jdbcTemplate.queryForObject("SELECT message_id FROM payment_outbox LIMIT 1", String.class);
        String payload = jdbcTemplate.queryForObject("SELECT payload FROM payment_outbox LIMIT 1", String.class);
        UUID messageId = UUID.fromString(messageIdStr);

        paymentConsumer.processIdempotently(messageId, payload);
        
        // Confirm balance is STILL 100.00 after attempting duplicate process
        BigDecimal balanceAfterDuplicate = jdbcTemplate.queryForObject(
                "SELECT balance FROM target_accounts WHERE id = ?", BigDecimal.class, targetAccount);
        assertEquals(0, new BigDecimal("100.00").compareTo(balanceAfterDuplicate), "Balance should remain 100.00 due to idempotency");

        // Confirm only 1 strictly processed message
        Integer processedCount = jdbcTemplate.queryForObject("SELECT count(*) FROM processed_messages", Integer.class);
        assertEquals(1, processedCount, "There should still be only 1 processed message");
    }
}
