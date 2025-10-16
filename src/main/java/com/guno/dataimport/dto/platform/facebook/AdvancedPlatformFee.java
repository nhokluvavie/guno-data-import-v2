package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * AdvancedPlatformFee - Platform fees breakdown
 * Used by Shopee, TikTok (Facebook returns empty object)
 *
 * Location: src/main/java/com/guno/dataimport/dto/platform/facebook/AdvancedPlatformFee.java
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdvancedPlatformFee {

    @JsonProperty("tax")
    private Long tax;

    @JsonProperty("payment_fee")
    private Long paymentFee;

    @JsonProperty("service_fee")
    private Long serviceFee;

    @JsonProperty("seller_transaction_fee")
    private Long sellerTransactionFee;

    /**
     * Calculate total platform fee
     * @return Sum of all fee components
     */
    public Double getTotalFee() {
        double total = 0.0;
        if (tax != null) total += tax;
        if (paymentFee != null) total += paymentFee;
        if (serviceFee != null) total += serviceFee;
        if (sellerTransactionFee != null) total += sellerTransactionFee;
        return total;
    }

    /**
     * Check if fee data exists
     * @return true if at least one fee component is present
     */
    public boolean hasData() {
        return tax != null || paymentFee != null ||
                serviceFee != null || sellerTransactionFee != null;
    }
}