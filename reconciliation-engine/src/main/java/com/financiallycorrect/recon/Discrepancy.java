package com.financiallycorrect.recon;

import lombok.Value;

@Value
public class Discrepancy {
    String transactionId;
    Type type;
    String details;

    public enum Type {
        MISSING_IN_INTERNAL,
        MISSING_IN_STRIPE,
        AMOUNT_MISMATCH,
        DUPLICATE_IN_INTERNAL,
        DUPLICATE_IN_STRIPE
    }
}
