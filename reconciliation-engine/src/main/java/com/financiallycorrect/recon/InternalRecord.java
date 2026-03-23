package com.financiallycorrect.recon;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class InternalRecord {
    @CsvBindByName(column = "transaction_id")
    private String transactionId;

    @CsvBindByName(column = "amount")
    private BigDecimal amount;

    @CsvBindByName(column = "currency")
    private String currency;
}
