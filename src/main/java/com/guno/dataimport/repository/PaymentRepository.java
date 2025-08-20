package com.guno.dataimport.repository;

import com.guno.dataimport.entity.PaymentInfo;
import com.guno.dataimport.util.CsvFormatter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Payment Repository - JDBC operations with COPY FROM optimization
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

    private static final String COPY_SQL = """
        COPY tbl_payment_info (
            order_id, payment_key, payment_method, payment_category, payment_provider,
            is_cod, is_prepaid, is_installment, installment_months, supports_refund,
            supports_partial_refund, refund_processing_days, risk_level,
            requires_verification, fraud_score, transaction_fee_rate, processing_fee,
            payment_processing_time_minutes, settlement_days
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<PaymentInfo> paymentInfos) {
        if (paymentInfos == null || paymentInfos.isEmpty()) return 0;
        try {
            return tempTableUpsert(paymentInfos, "tbl_payment_info",
                    "order_id", "payment_method = EXCLUDED.payment_method, is_cod = EXCLUDED.is_cod");
        } catch (Exception e) {
            log.warn("Temp table failed, using batch: {}", e.getMessage());
            return executeBatchUpsert(paymentInfos);
        }
    }

    public List<PaymentInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_payment_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, paymentRowMapper(), orderIds.toArray(new String[0]));
    }

    public PaymentInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_payment_info WHERE order_id = ?";
        List<PaymentInfo> results = jdbcTemplate.query(sql, paymentRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_payment_info", Long.class);
    }

    // === COPY FROM Implementation ===

    public int bulkInsertWithCopy(List<PaymentInfo> paymentInfos) throws Exception {
        log.info("Bulk inserting {} payment records using COPY FROM", paymentInfos.size());

        String csvData = generateCsvData(paymentInfos);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    // Update existing deleteByOrderIds method
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds.isEmpty()) return 0;

        if (orderIds.size() <= 1000) {
            String placeholders = String.join(",", Collections.nCopies(orderIds.size(), "?"));
            String sql = "DELETE FROM tbl_payment_info WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_payment_info WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private <T> int tempTableUpsert(List<PaymentInfo> entities, String tableName, String conflictColumns, String updateSet) throws Exception {
        String tempTable = "temp_" + tableName.substring(4) + "_" + System.currentTimeMillis();

        try {
            // Create temp table
            jdbcTemplate.execute("CREATE TEMP TABLE " + tempTable + " (LIKE " + tableName + " INCLUDING DEFAULTS)");

            // COPY INTO temp table
            String tempCopySQL = COPY_SQL.replace(tableName, tempTable);
            String csvData = generateCsvData(entities);

            jdbcTemplate.execute((Connection conn) -> {
                CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
                try (StringReader reader = new StringReader(csvData)) {
                    return (int) copyManager.copyIn(tempCopySQL, reader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            // MERGE to main table
            String mergeSQL = String.format(
                    "INSERT INTO %s SELECT * FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
                    tableName, tempTable, conflictColumns, updateSet);

            return jdbcTemplate.update(mergeSQL);

        } finally {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + tempTable);
        }
    }

    private String generateCsvData(List<PaymentInfo> paymentInfos) {
        return paymentInfos.stream()
                .map(payment -> CsvFormatter.joinCsvRow(
                        payment.getOrderId(), payment.getPaymentKey(), payment.getPaymentMethod(),
                        payment.getPaymentCategory(), payment.getPaymentProvider(),
                        CsvFormatter.formatBoolean(payment.getIsCod()), CsvFormatter.formatBoolean(payment.getIsPrepaid()),
                        CsvFormatter.formatBoolean(payment.getIsInstallment()), payment.getInstallmentMonths(),
                        CsvFormatter.formatBoolean(payment.getSupportsRefund()), CsvFormatter.formatBoolean(payment.getSupportsPartialRefund()),
                        payment.getRefundProcessingDays(), payment.getRiskLevel(),
                        CsvFormatter.formatBoolean(payment.getRequiresVerification()), payment.getFraudScore(),
                        payment.getTransactionFeeRate(), payment.getProcessingFee(),
                        payment.getPaymentProcessingTimeMinutes(), payment.getSettlementDays()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<PaymentInfo> paymentInfos) {
        log.info("Batch upserting {} payment records", paymentInfos.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, paymentInfos.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(PaymentInfo p) {
        return new Object[]{
                p.getOrderId(), p.getPaymentKey(), p.getPaymentMethod(), p.getPaymentCategory(),
                p.getPaymentProvider(), p.getIsCod(), p.getIsPrepaid(), p.getIsInstallment(),
                p.getInstallmentMonths(), p.getSupportsRefund(), p.getSupportsPartialRefund(),
                p.getRefundProcessingDays(), p.getRiskLevel(), p.getRequiresVerification(),
                p.getFraudScore(), p.getTransactionFeeRate(), p.getProcessingFee(),
                p.getPaymentProcessingTimeMinutes(), p.getSettlementDays()
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