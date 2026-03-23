package com.financiallycorrect.recon;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconciliationServiceTest {

    private final ReconciliationService service = new ReconciliationService();

    @Test
    void testReconciliation() {
        String stripeCsv = "id,amount,currency,status\n" +
                "tx_1,100.00,usd,paid\n" + // Match
                "tx_2,50.00,usd,paid\n" +  // Amount mismatch
                "tx_3,75.00,usd,paid\n" +  // Missing in internal
                "tx_4,10.00,usd,paid\n" +  // Duplicate in stripe
                "tx_4,10.00,usd,paid\n" +
                "tx_6,20.00,usd,failed\n"; // Ignored by status

        String internalCsv = "transaction_id,amount,currency\n" +
                "tx_1,100.00,usd\n" +      // Match
                "tx_2,40.00,usd\n" +       // Amount mismatch
                "tx_4,10.00,usd\n" +       // Match to duplicate stripe
                "tx_5,200.00,usd\n" +      // Missing in stripe
                "tx_5,200.00,usd\n" +      // Duplicate internal
                "tx_6,20.00,usd\n";        // Exists internally but not successful in stripe

        List<Discrepancy> discrepancies = service.reconcile(new StringReader(stripeCsv), new StringReader(internalCsv));

        boolean foundAmountMismatch = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_2") && d.getType() == Discrepancy.Type.AMOUNT_MISMATCH);
        assertTrue(foundAmountMismatch, "Should detect AMOUNT_MISMATCH");

        boolean foundMissingInternal = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_3") && d.getType() == Discrepancy.Type.MISSING_IN_INTERNAL);
        assertTrue(foundMissingInternal, "Should detect MISSING_IN_INTERNAL");

        boolean foundDuplicateStripe = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_4") && d.getType() == Discrepancy.Type.DUPLICATE_IN_STRIPE);
        assertTrue(foundDuplicateStripe, "Should detect DUPLICATE_IN_STRIPE");

        boolean foundDuplicateInternal = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_5") && d.getType() == Discrepancy.Type.DUPLICATE_IN_INTERNAL);
        assertTrue(foundDuplicateInternal, "Should detect DUPLICATE_IN_INTERNAL");

        boolean foundMissingStripe = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_5") && d.getType() == Discrepancy.Type.MISSING_IN_STRIPE);
        assertTrue(foundMissingStripe, "Should detect MISSING_IN_STRIPE");
        
        boolean foundTx6MissingInStripe = discrepancies.stream().anyMatch(d -> 
            d.getTransactionId().equals("tx_6") && d.getType() == Discrepancy.Type.MISSING_IN_STRIPE);
        assertTrue(foundTx6MissingInStripe, "tx_6 failed in Stripe, so internal tx_6 should be marked MISSING_IN_STRIPE");

        // Total expected:
        // tx_2 AMOUNT_MISMATCH
        // tx_3 MISSING_IN_INTERNAL
        // tx_4 DUPLICATE_IN_STRIPE
        // tx_5 DUPLICATE_IN_INTERNAL
        // tx_5 MISSING_IN_STRIPE
        // tx_6 MISSING_IN_STRIPE
        assertEquals(6, discrepancies.size());
    }
}
