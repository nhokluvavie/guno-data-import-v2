package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Status;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Status Repository - JDBC operations for Status entity (Master table)
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class StatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_status (
            status_key, platform, platform_status_code, platform_status_name,
            standard_status_code, standard_status_name, status_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (status_key) DO UPDATE SET
            platform_status_name = EXCLUDED.platform_status_name,
            standard_status_code = EXCLUDED.standard_status_code,
            standard_status_name = EXCLUDED.standard_status_name,
            status_category = EXCLUDED.status_category
        """;

    // Bulk upsert status records
    public int bulkUpsert(List<Status> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} status records", statuses.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, statuses.stream()
                .map(this::mapStatusToParams)
                .toList()
        ).length;
    }

    // Find status by keys
    public Map<Long, Status> findByKeys(Set<Long> statusKeys) {
        if (statusKeys == null || statusKeys.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT * FROM tbl_status WHERE status_key = ANY(?)";
        List<Status> statuses = jdbcTemplate.query(sql, statusRowMapper(),
                statusKeys.toArray(new Long[0]));

        return statuses.stream()
                .collect(java.util.stream.Collectors.toMap(
                        Status::getStatusKey,
                        status -> status
                ));
    }

    // Find by platform and status code
    public Status findByPlatformAndCode(String platform, String platformStatusCode) {
        String sql = "SELECT * FROM tbl_status WHERE platform = ? AND platform_status_code = ?";
        List<Status> results = jdbcTemplate.query(sql, statusRowMapper(), platform, platformStatusCode);
        return results.isEmpty() ? null : results.get(0);
    }

    // Find all statuses by platform
    public List<Status> findByPlatform(String platform) {
        String sql = "SELECT * FROM tbl_status WHERE platform = ? ORDER BY status_key";
        return jdbcTemplate.query(sql, statusRowMapper(), platform);
    }

    // Check if status exists
    public boolean exists(Long statusKey) {
        String sql = "SELECT 1 FROM tbl_status WHERE status_key = ?";
        return !jdbcTemplate.queryForList(sql, statusKey).isEmpty();
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_status", Long.class);
    }

    // Get next status key (for auto-generation)
    public Long getNextStatusKey() {
        String sql = "SELECT COALESCE(MAX(status_key), 0) + 1 FROM tbl_status";
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    // Helper methods
    private Object[] mapStatusToParams(Status status) {
        return new Object[]{
                status.getStatusKey(),
                status.getPlatform(),
                status.getPlatformStatusCode(),
                status.getPlatformStatusName(),
                status.getStandardStatusCode(),
                status.getStandardStatusName(),
                status.getStatusCategory()
        };
    }

    private RowMapper<Status> statusRowMapper() {
        return (rs, rowNum) -> Status.builder()
                .statusKey(rs.getLong("status_key"))
                .platform(rs.getString("platform"))
                .platformStatusCode(rs.getString("platform_status_code"))
                .platformStatusName(rs.getString("platform_status_name"))
                .standardStatusCode(rs.getString("standard_status_code"))
                .standardStatusName(rs.getString("standard_status_name"))
                .statusCategory(rs.getString("status_category"))
                .build();
    }
}