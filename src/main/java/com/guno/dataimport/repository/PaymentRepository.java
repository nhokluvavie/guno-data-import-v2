package com.guno.dataimport.repository;

import com.guno.dataimport.entity.PaymentInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * Payment Repository - JDBC operations for PaymentInfo entity
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class PaymentRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_payment_info (
            order_id, payment_key, payment_method, payment_category, payment_provider,
            is_cod, is_prepaid, is_installment, installment_months, supports_refund,
            supports_partial_refund, refund_processing_days, risk_level,
            requires_verification, fraud_score, transaction_fee_rate, processing_fee,
            payment_processing_time_minutes, settlement_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            payment_method = EXCLUDED.payment_method,
            payment_category = EXCLUDED.payment_category,
            payment_provider = EXCLUDED.payment_provider,
            is_cod = EXCLUDED.is_cod,
            is_prepaid = EXCLUDED.is_prepaid,
            processing_fee = EXCLUDED.processing_fee,
            fraud_score = EXCLUDED.fraud_score
        """;

    // Bulk upsert payment info
    public int bulkUpsert(List<PaymentInfo> paymentInfos) {
        if (paymentInfos == null || paymentInfos.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} payment records", paymentInfos.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, paymentInfos.stream()
                .map(this::mapPaymentToParams)
                .toList()
        ).length;
    }

    // Find payment info by order IDs
    public List<PaymentInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_payment_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, paymentRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public PaymentInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_payment_info WHERE order_id = ?";
        List<PaymentInfo> results = jdbcTemplate.query(sql, paymentRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_payment_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_payment_info", Long.class);
    }

    // Helper methods
    private Object[] mapPaymentToParams(PaymentInfo payment) {
        return new Object[]{
                payment.getOrderId(),
                payment.getPaymentKey(),
                payment.getPaymentMethod(),
                payment.getPaymentCategory(),
                payment.getPaymentProvider(),
                payment.getIsCod(),
                payment.getIsPrepaid(),
                payment.getIsInstallment(),
                payment.getInstallmentMonths(),
                payment.getSupportsRefund(),
                payment.getSupportsPartialRefund(),
                payment.getRefundProcessingDays(),
                payment.getRiskLevel(),
                payment.getRequiresVerification(),
                payment.getFraudScore(),
                payment.getTransactionFeeRate(),
                payment.getProcessingFee(),
                payment.getPaymentProcessingTimeMinutes(),
                payment.getSettlementDays()
        };
    }

    private RowMapper<PaymentInfo> paymentRowMapper() {
        return (rs, rowNum) -> PaymentInfo.builder()
                .orderId(rs.getString("order_id"))
                .paymentKey(rs.getLong("payment_key"))
                .paymentMethod(rs.getString("payment_method"))
                .paymentCategory(rs.getString("payment_category"))
                .paymentProvider(rs.getString("payment_provider"))
                .isCod(rs.getBoolean("is_cod"))
                .isPrepaid(rs.getBoolean("is_prepaid"))
                .isInstallment(rs.getBoolean("is_installment"))
                .installmentMonths(rs.getInt("installment_months"))
                .supportsRefund(rs.getBoolean("supports_refund"))
                .supportsPartialRefund(rs.getBoolean("supports_partial_refund"))
                .refundProcessingDays(rs.getInt("refund_processing_days"))
                .riskLevel(rs.getString("risk_level"))
                .requiresVerification(rs.getBoolean("requires_verification"))
                .fraudScore(rs.getDouble("fraud_score"))
                .transactionFeeRate(rs.getDouble("transaction_fee_rate"))
                .processingFee(rs.getDouble("processing_fee"))
                .paymentProcessingTimeMinutes(rs.getInt("payment_processing_time_minutes"))
                .settlementDays(rs.getInt("settlement_days"))
                .build();
    }
}