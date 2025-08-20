package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Status;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * Status Repository - JDBC operations with COPY FROM optimization (Master table)
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

    private static final String COPY_SQL = """
        COPY tbl_status (
            status_key, platform, platform_status_code, platform_status_name,
            standard_status_code, standard_status_name, status_category
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<Status> statuses) {
        if (statuses == null || statuses.isEmpty()) return 0;
        try {
            return tempTableUpsert(statuses, "tbl_status",
                    "status_key", "platform_status_name = EXCLUDED.platform_status_name");
        } catch (Exception e) {
            log.warn("Temp table failed, using batch: {}", e.getMessage());
            return executeBatchUpsert(statuses);
        }
    }

    public Map<Long, Status> findByKeys(Set<Long> statusKeys) {
        if (statusKeys == null || statusKeys.isEmpty()) return Map.of();

        String sql = "SELECT * FROM tbl_status WHERE status_key = ANY(?)";
        return jdbcTemplate.query(sql, statusRowMapper(), statusKeys.toArray(new Long[0]))
                .stream().collect(java.util.stream.Collectors.toMap(
                        Status::getStatusKey, status -> status));
    }

    public Status findByPlatformAndCode(String platform, String platformStatusCode) {
        String sql = "SELECT * FROM tbl_status WHERE platform = ? AND platform_status_code = ?";
        List<Status> results = jdbcTemplate.query(sql, statusRowMapper(), platform, platformStatusCode);
        return results.isEmpty() ? null : results.get(0);
    }

    public List<Status> findByPlatform(String platform) {
        String sql = "SELECT * FROM tbl_status WHERE platform = ? ORDER BY status_key";
        return jdbcTemplate.query(sql, statusRowMapper(), platform);
    }

    public boolean exists(Long statusKey) {
        return !jdbcTemplate.queryForList("SELECT 1 FROM tbl_status WHERE status_key = ?", statusKey).isEmpty();
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_status", Long.class);
    }

    public Long getNextStatusKey() {
        String sql = "SELECT COALESCE(MAX(status_key), 0) + 1 FROM tbl_status";
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    // === COPY FROM Implementation ===

    public int bulkInsertWithCopy(List<Status> statuses) throws Exception {
        log.info("Bulk inserting {} status records using COPY FROM", statuses.size());

        String csvData = generateCsvData(statuses);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int deleteByKeys(Set<Long> statusKeys) {
        if (statusKeys.isEmpty()) return 0;

        if (statusKeys.size() <= 1000) {
            String placeholders = String.join(",", Collections.nCopies(statusKeys.size(), "?"));
            String sql = "DELETE FROM tbl_status WHERE status_key IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, statusKeys.toArray());
        }

        List<Long> keyList = new ArrayList<>(statusKeys);
        int totalDeleted = 0;
        for (int i = 0; i < keyList.size(); i += 1000) {
            List<Long> batch = keyList.subList(i, Math.min(i + 1000, keyList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_status WHERE status_key IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private <T> int tempTableUpsert(List<Status> entities, String tableName, String conflictColumns, String updateSet) throws Exception {
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

    private String generateCsvData(List<Status> statuses) {
        return statuses.stream()
                .map(status -> CsvFormatter.joinCsvRow(
                        status.getStatusKey(), status.getPlatform(), status.getPlatformStatusCode(),
                        status.getPlatformStatusName(), status.getStandardStatusCode(),
                        status.getStandardStatusName(), status.getStatusCategory()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<Status> statuses) {
        log.info("Batch upserting {} status records", statuses.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, statuses.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(Status s) {
        return new Object[]{
                s.getStatusKey(), s.getPlatform(), s.getPlatformStatusCode(),
                s.getPlatformStatusName(), s.getStandardStatusCode(),
                s.getStandardStatusName(), s.getStatusCategory()
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