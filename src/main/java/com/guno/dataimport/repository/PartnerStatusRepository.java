package com.guno.dataimport.repository;

import com.guno.dataimport.entity.PartnerStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * PartnerStatus Repository - JDBC operations for tbl_partner_status
 * UPDATED: Support Integer ID instead of String
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class PartnerStatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_partner_status (id, partner_status_name, stage)
        VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            partner_status_name = EXCLUDED.partner_status_name,
            stage = EXCLUDED.stage
        """;

    /**
     * Bulk upsert PartnerStatus records
     */
    public int bulkUpsert(List<PartnerStatus> partnerStatuses) {
        if (partnerStatuses == null || partnerStatuses.isEmpty()) return 0;

        int[] results = jdbcTemplate.batchUpdate(UPSERT_SQL,
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(java.sql.PreparedStatement ps, int i)
                            throws java.sql.SQLException {
                        PartnerStatus p = partnerStatuses.get(i);
                        ps.setInt(1, p.getId());           // CHANGED: setString → setInt
                        ps.setString(2, p.getPartnerStatusName());
                        ps.setString(3, p.getStage());
                    }

                    @Override
                    public int getBatchSize() {
                        return partnerStatuses.size();
                    }
                });

        log.info("Upserted {} PartnerStatus records", results.length);
        return results.length;
    }

    /**
     * Find all partner statuses
     */
    public List<PartnerStatus> findAll() {
        return jdbcTemplate.query(
                "SELECT * FROM tbl_partner_status ORDER BY id",
                partnerStatusRowMapper()
        );
    }

    /**
     * Find partner status by ID
     */
    public PartnerStatus findById(Integer id) {  // CHANGED: String → Integer
        String sql = "SELECT * FROM tbl_partner_status WHERE id = ?";
        List<PartnerStatus> results = jdbcTemplate.query(sql, partnerStatusRowMapper(), id);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Find partner statuses by multiple IDs
     */
    public List<PartnerStatus> findByIds(Set<Integer> ids) {  // CHANGED: String → Integer
        if (ids == null || ids.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_partner_status WHERE id = ANY(?)";
        return jdbcTemplate.query(sql, partnerStatusRowMapper(),
                ids.toArray(new Integer[0]));
    }

    /**
     * Find partner statuses by stage
     */
    public List<PartnerStatus> findByStage(String stage) {
        String sql = "SELECT * FROM tbl_partner_status WHERE stage = ?";
        return jdbcTemplate.query(sql, partnerStatusRowMapper(), stage);
    }

    /**
     * Check if partner status exists
     */
    public boolean exists(Integer id) {  // CHANGED: String → Integer
        String sql = "SELECT COUNT(*) FROM tbl_partner_status WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, id);
        return count != null && count > 0;
    }

    /**
     * RowMapper for PartnerStatus
     */
    private RowMapper<PartnerStatus> partnerStatusRowMapper() {
        return (rs, rowNum) -> PartnerStatus.builder()
                .id(rs.getInt("id"))  // CHANGED: getString → getInt
                .partnerStatusName(rs.getString("partner_status_name"))
                .stage(rs.getString("stage"))
                .build();
    }
}