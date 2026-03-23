package com.financiallycorrect.ledger;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Service
public class LedgerService {

    private final JdbcTemplate jdbcTemplate;

    public LedgerService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public UUID createAccount(String name) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO accounts (id, name) VALUES (?, ?)", id, name);
        return id;
    }

    @Transactional
    public UUID recordTransaction(String reference, String description, Map<UUID, BigDecimal> entries) {
        UUID txId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO transactions (id, reference, description) VALUES (?, ?, ?)", txId, reference, description);

        String entrySql = "INSERT INTO ledger_entries (id, transaction_id, account_id, amount) VALUES (?, ?, ?, ?)";
        for (Map.Entry<UUID, BigDecimal> entry : entries.entrySet()) {
            jdbcTemplate.update(entrySql, UUID.randomUUID(), txId, entry.getKey(), entry.getValue());
        }
        
        // The DB deferred trigger `trigger_check_transaction_balance` will verify the transaction sum == 0 on commit.
        return txId;
    }

    @Transactional(readOnly = true)
    public BigDecimal getBalance(UUID accountId) {
        // 1. Get latest snapshot
        String snapshotSql = "SELECT balance, last_sequence_num FROM account_snapshots WHERE account_id = ? ORDER BY last_sequence_num DESC LIMIT 1";
        
        BigDecimal snapshotBalance = BigDecimal.ZERO;
        long lastSeq = 0;
        
        try {
            Map<String, Object> snapshot = jdbcTemplate.queryForMap(snapshotSql, accountId);
            snapshotBalance = (BigDecimal) snapshot.get("balance");
            lastSeq = ((Number) snapshot.get("last_sequence_num")).longValue();
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            // No snapshot exists, proceed with 0 balance and 0 sequence
        }

        // 2. Replay entries since last snapshot
        String replaySql = "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ? AND sequence_num > ?";
        BigDecimal replayBalance = jdbcTemplate.queryForObject(replaySql, BigDecimal.class, accountId, lastSeq);

        return snapshotBalance.add(replayBalance);
    }

    @Transactional
    public void createSnapshot(UUID accountId) {
        // Get the maximum fully committed sequence for this account
        String maxSeqSql = "SELECT COALESCE(MAX(sequence_num), 0) FROM ledger_entries WHERE account_id = ?";
        long maxSeq = jdbcTemplate.queryForObject(maxSeqSql, Long.class, accountId);
        
        if (maxSeq == 0) return; // Nothing to snapshot

        // Calculate total balance up to this sequence to avoid sequence gap issues with concurrent uncommitted txs
        String totalSql = "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ? AND sequence_num <= ?";
        BigDecimal totalBalance = jdbcTemplate.queryForObject(totalSql, BigDecimal.class, accountId, maxSeq);

        String insertSnapshot = "INSERT INTO account_snapshots (id, account_id, balance, last_sequence_num) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(insertSnapshot, UUID.randomUUID(), accountId, totalBalance, maxSeq);
    }
}
