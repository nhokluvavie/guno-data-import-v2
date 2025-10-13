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
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class PartnerStatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_partner_status (id, partner_status_name, is_returned)
        VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            partner_status_name = EXCLUDED.partner_status_name,
            is_returned = EXCLUDED.is_returned
        """;

    /**
     * Bulk upsert PartnerStatus records
     */
    public int bulkUpsert(List<PartnerStatus> partnerStatuses) {
        if (partnerStatuses == null || partnerStatuses.isEmpty()) return 0;

        int[] results = jdbcTemplate.batchUpdate(UPSERT_SQL,
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                        PartnerStatus p = partnerStatuses.get(i);
                        ps.setString(1, p.getId());
                        ps.setString(2, p.getPartnerStatusName());
                        ps.setBoolean(3, p.getIsReturned());
                    }

                    @Override
                    public int getBatchSize() {
                        return partnerStatuses.size();
                    }
                });

        log.info("Upserted {} PartnerStatus records", results.length);
        return results.length;
    }

    public List<PartnerStatus> findAll() {
        return jdbcTemplate.query("SELECT * FROM tbl_partner_status", partnerStatusRowMapper());
    }

    public PartnerStatus findById(String id) {
        String sql = "SELECT * FROM tbl_partner_status WHERE id = ?";
        List<PartnerStatus> results = jdbcTemplate.query(sql, partnerStatusRowMapper(), id);
        return results.isEmpty() ? null : results.get(0);
    }

    public List<PartnerStatus> findByIds(Set<String> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        String sql = "SELECT * FROM tbl_partner_status WHERE id = ANY(?)";
        return jdbcTemplate.query(sql, partnerStatusRowMapper(), ids.toArray(new String[0]));
    }

    public List<PartnerStatus> findReturned() {
        String sql = "SELECT * FROM tbl_partner_status WHERE is_returned = true";
        return jdbcTemplate.query(sql, partnerStatusRowMapper());
    }

    private RowMapper<PartnerStatus> partnerStatusRowMapper() {
        return (rs, rowNum) -> PartnerStatus.builder()
                .id(rs.getString("id"))
                .partnerStatusName(rs.getString("partner_status_name"))
                .isReturned(rs.getBoolean("is_returned"))
                .build();
    }
}