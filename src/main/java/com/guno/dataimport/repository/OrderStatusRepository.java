package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * OrderStatus Repository - JDBC operations
 * UPDATED: Composite key changed to (status_key, order_id, sub_status_id, partner_status_id)
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderStatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_order_status (
            status_key, order_id, sub_status_id, partner_status_id,
            transition_date_key, transition_timestamp, duration_in_previous_status_hours,
            transition_reason, transition_trigger, changed_by,
            is_on_time_transition, is_expected_transition, history_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (status_key, order_id, sub_status_id, partner_status_id) DO UPDATE SET
            transition_timestamp = EXCLUDED.transition_timestamp,
            duration_in_previous_status_hours = EXCLUDED.duration_in_previous_status_hours,
            transition_reason = EXCLUDED.transition_reason,
            is_on_time_transition = EXCLUDED.is_on_time_transition
        """;

    /**
     * Bulk upsert OrderStatus records
     */
    public int bulkUpsert(List<OrderStatus> orderStatuses) {
        if (orderStatuses == null || orderStatuses.isEmpty()) {
            return 0;
        }

        final int CHUNK_SIZE = 1000;
        int totalProcessed = 0;

        log.info("📦 Batch upserting {} order statuses in chunks of {}",
                orderStatuses.size(), CHUNK_SIZE);

        for (int i = 0; i < orderStatuses.size(); i += CHUNK_SIZE) {
            int end = Math.min(i + CHUNK_SIZE, orderStatuses.size());
            List<OrderStatus> chunk = orderStatuses.subList(i, end);

            log.debug("   OrderStatus chunk {}-{} of {}", i + 1, end, orderStatuses.size());

            // Process this chunk
            int[] results = jdbcTemplate.batchUpdate(UPSERT_SQL,
                    new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(java.sql.PreparedStatement ps, int idx) throws java.sql.SQLException {
                            OrderStatus os = chunk.get(idx);  // ← Use chunk, not orderStatuses
                            ps.setLong(1, os.getStatusKey());
                            ps.setString(2, os.getOrderId());
                            ps.setString(3, os.getSubStatusId());
                            ps.setString(4, String.valueOf(os.getPartnerStatusId()));
                            ps.setInt(5, os.getTransitionDateKey());
                            ps.setObject(6, os.getTransitionTimestamp());
                            ps.setInt(7, os.getDurationInPreviousStatusHours());
                            ps.setString(8, os.getTransitionReason());
                            ps.setString(9, os.getTransitionTrigger());
                            ps.setString(10, os.getChangedBy());
                            ps.setBoolean(11, os.getIsOnTimeTransition());
                            ps.setBoolean(12, os.getIsExpectedTransition());
                            ps.setLong(13, os.getHistoryKey());
                            ps.setString(14, os.getCreatedAt());
                        }

                        @Override
                        public int getBatchSize() {
                            return chunk.size();  // ← Chunk size, not full list
                        }
                    });

            totalProcessed += results.length;
        }

        log.info("✅ OrderStatus batch completed: {} records", totalProcessed);
        return totalProcessed;
    }

    public List<OrderStatus> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();
        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), orderIds.toArray(new String[0]));
    }

    public List<OrderStatus> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ? ORDER BY transition_timestamp DESC";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), orderId);
    }

    public OrderStatus findByCompositeKey(Long statusKey, String orderId, String subStatusId, String partnerStatusId) {
        String sql = """
            SELECT * FROM tbl_order_status 
            WHERE status_key = ? AND order_id = ? AND sub_status_id = ? AND partner_status_id = ?
            """;
        List<OrderStatus> results = jdbcTemplate.query(sql, orderStatusRowMapper(),
                statusKey, orderId, subStatusId, partnerStatusId);
        return results.isEmpty() ? null : results.get(0);
    }

    public List<OrderStatus> findByStatusKey(Long statusKey) {
        String sql = "SELECT * FROM tbl_order_status WHERE status_key = ?";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), statusKey);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_status", Long.class);
    }

    private RowMapper<OrderStatus> orderStatusRowMapper() {
        return (rs, rowNum) -> OrderStatus.builder()
                .statusKey(rs.getLong("status_key"))
                .orderId(rs.getString("order_id"))
                .subStatusId(rs.getString("sub_status_id"))
                .partnerStatusId(Integer.valueOf(rs.getString("partner_status_id")))
                .transitionDateKey(rs.getInt("transition_date_key"))
                .transitionTimestamp(rs.getTimestamp("transition_timestamp") != null
                        ? rs.getTimestamp("transition_timestamp").toLocalDateTime() : null)
                .durationInPreviousStatusHours(rs.getInt("duration_in_previous_status_hours"))
                .transitionReason(rs.getString("transition_reason"))
                .transitionTrigger(rs.getString("transition_trigger"))
                .changedBy(rs.getString("changed_by"))
                .isOnTimeTransition(rs.getBoolean("is_on_time_transition"))
                .isExpectedTransition(rs.getBoolean("is_expected_transition"))
                .historyKey(rs.getLong("history_key"))
                .createdAt(rs.getString("created_at"))
                .build();
    }
}