package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * PaymentInfo Entity - Maps to tbl_payment_info table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentInfo {

    private String orderId;
    private Long paymentKey;

    @Builder.Default private String paymentMethod = "";
    @Builder.Default private String paymentCategory = "";
    @Builder.Default private String paymentProvider = "";

    @Builder.Default private Boolean isCod = false;
    @Builder.Default private Boolean isPrepaid = false;
    @Builder.Default private Boolean isInstallment = false;
    @Builder.Default private Integer installmentMonths = 0;

    // Refund capabilities
    @Builder.Default private Boolean supportsRefund = false;
    @Builder.Default private Boolean supportsPartialRefund = false;
    @Builder.Default private Integer refundProcessingDays = 0;

    // Risk and verification
    @Builder.Default private String riskLevel = "";
    @Builder.Default private Boolean requiresVerification = false;
    @Builder.Default private Double fraudScore = 0.0;

    // Fee structure
    @Builder.Default private Double transactionFeeRate = 0.0;
    @Builder.Default private Double processingFee = 0.0;
    @Builder.Default private Integer paymentProcessingTimeMinutes = 0;
    @Builder.Default private Integer settlementDays = 0;
}