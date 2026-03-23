package com.financiallycorrect.ledger;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
class LedgerConcurrencyTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("test_ledger")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private LedgerService ledgerService;

    @Test
    void testConcurrentTransfersAndSnapshots() throws InterruptedException {
        UUID account1 = ledgerService.createAccount("Alice");
        UUID account2 = ledgerService.createAccount("Bob");

        // Initial deposit to Alice: 1000
        Map<UUID, BigDecimal> initialDeposit = new HashMap<>();
        initialDeposit.put(account1, new BigDecimal("1000.00"));
        UUID bankAccount = ledgerService.createAccount("Bank");
        initialDeposit.put(bankAccount, new BigDecimal("-1000.00"));
        ledgerService.recordTransaction("TX-INIT", "Initial Deposit", initialDeposit);

        int threads = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        AtomicInteger successfulTransfers = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    Map<UUID, BigDecimal> transfer = new HashMap<>();
                    transfer.put(account1, new BigDecimal("-5.00")); // Alice sends 5
                    transfer.put(account2, new BigDecimal("5.00"));  // Bob receives 5
                    ledgerService.recordTransaction("TX-" + index + "-" + Thread.currentThread().getId(), "Transfer " + index, transfer);
                    successfulTransfers.incrementAndGet();
                    
                    // Take snapshots concurrently to ensure no deadlocks or errors
                    if (index % 10 == 0) {
                        ledgerService.createSnapshot(account1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertEquals(100, successfulTransfers.get(), "All 100 transfers should succeed concurrently");

        // Alice started with 1000, sent 5 * 100 = 500. Expected: 500
        // Bob started with 0, received 5 * 100 = 500. Expected: 500
        BigDecimal aliceBalance = ledgerService.getBalance(account1);
        BigDecimal bobBalance = ledgerService.getBalance(account2);

        assertEquals(0, new BigDecimal("500.00").compareTo(aliceBalance));
        assertEquals(0, new BigDecimal("500.00").compareTo(bobBalance));
    }

    @Test
    void testConstraintViolatesUnbalancedTransaction() {
        UUID account1 = ledgerService.createAccount("Charlie");
        UUID account2 = ledgerService.createAccount("Dave");

        Map<UUID, BigDecimal> unbalanced = new HashMap<>();
        unbalanced.put(account1, new BigDecimal("-10.00"));
        unbalanced.put(account2, new BigDecimal("5.00")); // Sum is -5, not 0!

        Exception exception = assertThrows(DataIntegrityViolationException.class, () -> {
            ledgerService.recordTransaction("TX-UNBALANCED", "Bad transfer", unbalanced);
        });
        
        // Ensure exception is from DB trigger constraint
        assertTrue(exception.getMessage().contains("does not balance"), "Expected deferred trigger to throw balance exception");
    }
}
