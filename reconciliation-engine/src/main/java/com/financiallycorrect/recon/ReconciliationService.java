package com.financiallycorrect.recon;

import com.opencsv.bean.CsvToBeanBuilder;
import org.springframework.stereotype.Service;

import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ReconciliationService {

    public List<Discrepancy> reconcile(Reader stripeReader, Reader internalReader) {
        List<StripeRecord> stripeRecords = new CsvToBeanBuilder<StripeRecord>(stripeReader)
                .withType(StripeRecord.class).build().parse();
        
        List<InternalRecord> internalRecords = new CsvToBeanBuilder<InternalRecord>(internalReader)
                .withType(InternalRecord.class).build().parse();

        List<Discrepancy> discrepancies = new ArrayList<>();

        Map<String, List<InternalRecord>> internalMap = internalRecords.stream()
                .collect(Collectors.groupingBy(InternalRecord::getTransactionId));

        Map<String, List<StripeRecord>> stripeMap = stripeRecords.stream()
                .filter(r -> "paid".equalsIgnoreCase(r.getStatus()))
                .collect(Collectors.groupingBy(StripeRecord::getId));

        for (Map.Entry<String, List<InternalRecord>> entry : internalMap.entrySet()) {
            if (entry.getValue().size() > 1) {
                discrepancies.add(new Discrepancy(entry.getKey(), Discrepancy.Type.DUPLICATE_IN_INTERNAL, 
                    "Found " + entry.getValue().size() + " records"));
            }
        }

        for (Map.Entry<String, List<StripeRecord>> entry : stripeMap.entrySet()) {
            if (entry.getValue().size() > 1) {
                discrepancies.add(new Discrepancy(entry.getKey(), Discrepancy.Type.DUPLICATE_IN_STRIPE, 
                    "Found " + entry.getValue().size() + " records"));
            }
        }

        for (String stripeId : stripeMap.keySet()) {
            StripeRecord stripeTx = stripeMap.get(stripeId).get(0);
            if (!internalMap.containsKey(stripeId)) {
                discrepancies.add(new Discrepancy(stripeId, Discrepancy.Type.MISSING_IN_INTERNAL, "Exists in Stripe but not in internal DB"));
            } else {
                InternalRecord internalTx = internalMap.get(stripeId).get(0);
                if (stripeTx.getAmount().compareTo(internalTx.getAmount()) != 0) {
                    discrepancies.add(new Discrepancy(stripeId, Discrepancy.Type.AMOUNT_MISMATCH, 
                        String.format("Stripe: %s, Internal: %s", stripeTx.getAmount(), internalTx.getAmount())));
                }
            }
        }

        for (String internalId : internalMap.keySet()) {
            if (!stripeMap.containsKey(internalId)) {
                discrepancies.add(new Discrepancy(internalId, Discrepancy.Type.MISSING_IN_STRIPE, "Exists internally but not in Stripe settlement (or not paid)"));
            }
        }

        return discrepancies;
    }
}
