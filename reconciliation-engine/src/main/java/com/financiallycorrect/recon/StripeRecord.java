package com.financiallycorrect.recon;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class StripeRecord {
    @CsvBindByName(column = "id")
    private String id;

    @CsvBindByName(column = "amount")
    private BigDecimal amount;

    @CsvBindByName(column = "currency")
    private String currency;

    @CsvBindByName(column = "status")
    private String status;
}
