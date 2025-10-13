package com.guno.dataimport.repository;

import com.guno.dataimport.entity.SubStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

/**
 * SubStatus Repository - JDBC operations for tbl_substatus
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class SubStatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_substatus (id, sub_status_name)
        VALUES (?, ?)
        ON CONFLICT (id) DO UPDATE SET
            sub_status_name = EXCLUDED.sub_status_name
        """;

    /**
     * Bulk upsert SubStatus records
     */
    public int bulkUpsert(List<SubStatus> subStatuses) {
        if (subStatuses == null || subStatuses.isEmpty()) return 0;

        int[] results = jdbcTemplate.batchUpdate(UPSERT_SQL,
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                        SubStatus s = subStatuses.get(i);
                        ps.setString(1, s.getId());
                        ps.setString(2, s.getSubStatusName());
                    }

                    @Override
                    public int getBatchSize() {
                        return subStatuses.size();
                    }
                });

        log.info("Upserted {} SubStatus records", results.length);
        return results.length;
    }

    public List<SubStatus> findAll() {
        return jdbcTemplate.query("SELECT * FROM tbl_substatus", subStatusRowMapper());
    }

    public SubStatus findById(String id) {
        String sql = "SELECT * FROM tbl_substatus WHERE id = ?";
        List<SubStatus> results = jdbcTemplate.query(sql, subStatusRowMapper(), id);
        return results.isEmpty() ? null : results.get(0);
    }

    public List<SubStatus> findByIds(Set<String> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        String sql = "SELECT * FROM tbl_substatus WHERE id = ANY(?)";
        return jdbcTemplate.query(sql, subStatusRowMapper(), ids.toArray(new String[0]));
    }

    private RowMapper<SubStatus> subStatusRowMapper() {
        return (rs, rowNum) -> SubStatus.builder()
                .id(rs.getString("id"))
                .subStatusName(rs.getString("sub_status_name"))
                .build();
    }
}