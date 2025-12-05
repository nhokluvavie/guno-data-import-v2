package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Product;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Product Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ProductRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
    INSERT INTO tbl_product (
        sku, platform_product_id, product_id, variation_id, barcode, product_name,
        product_description, brand, model, category_level_1, category_level_2,
        category_level_3, category_path, color, "size", material, weight_gram,
        dimensions, cost_price, retail_price, original_price, price_range,
        is_active, is_featured, is_seasonal, is_new_arrival, is_best_seller,
        primary_image_url, image_count, seo_title, seo_keywords, sku_group
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (sku, platform_product_id) DO UPDATE SET
        product_name = EXCLUDED.product_name,
        retail_price = EXCLUDED.retail_price,
        original_price = EXCLUDED.original_price,
        is_active = EXCLUDED.is_active,
        primary_image_url = EXCLUDED.primary_image_url,
        image_count = EXCLUDED.image_count,
        sku_group = EXCLUDED.sku_group
    """;

    private static final String COPY_SQL = """
    COPY tbl_product (
        sku, platform_product_id, product_id, variation_id, barcode, product_name,
        product_description, brand, model, category_level_1, category_level_2,
        category_level_3, category_path, color, "size", material, weight_gram,
        dimensions, cost_price, retail_price, original_price, price_range,
        is_active, is_featured, is_seasonal, is_new_arrival, is_best_seller,
        primary_image_url, image_count, seo_title, seo_keywords, sku_group
    ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
    """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<Product> products) {
        if (products == null || products.isEmpty()) return 0;
        try {
            return tempTableUpsert(products, "tbl_product",
                    "sku, platform_product_id", "product_name = EXCLUDED.product_name, retail_price = EXCLUDED.retail_price");
        } catch (Exception e) {
            log.warn("Temp table failed, using batch: {}", e.getMessage());
            return executeBatchUpsert(products);
        }
    }

    /**
     * Find existing products by composite keys
     */
    public Map<String, Product> findByKeys(Set<String> compositeKeys) {
        if (compositeKeys == null || compositeKeys.isEmpty()) return Map.of();

        StringBuilder sql = new StringBuilder("SELECT * FROM tbl_product WHERE ");
        List<Object> params = new java.util.ArrayList<>();

        boolean first = true;
        for (String key : compositeKeys) {
            String[] parts = key.split("_", 2);
            if (parts.length == 2) {
                if (!first) sql.append(" OR ");
                sql.append("(sku = ? AND platform_product_id = ?)");
                params.add(parts[0]);
                params.add(parts[1]);
                first = false;
            }
        }

        return jdbcTemplate.query(sql.toString(), productRowMapper(), params.toArray())
                .stream().collect(java.util.stream.Collectors.toMap(
                        product -> product.getSku() + "_" + product.getPlatformProductId(),
                        product -> product));
    }

    public List<Product> findBySku(String sku) {
        return jdbcTemplate.query("SELECT * FROM tbl_product WHERE sku = ?", productRowMapper(), sku);
    }

    public boolean exists(String sku, String platformProductId) {
        return !jdbcTemplate.queryForList(
                "SELECT 1 FROM tbl_product WHERE sku = ? AND platform_product_id = ?",
                sku, platformProductId).isEmpty();
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_product", Long.class);
    }

    // === COPY FROM Implementation ===

    public int bulkInsertWithCopy(List<Product> products) throws Exception {
        log.info("Bulk inserting {} products using COPY FROM", products.size());

        String csvData = generateCsvData(products);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public int deleteByCompositeKeys(Set<String> compositeKeys) {
        if (compositeKeys.isEmpty()) return 0;

        if (compositeKeys.size() <= 500) {
            return deleteCompositeKeysBatch(new ArrayList<>(compositeKeys));
        }

        List<String> keyList = new ArrayList<>(compositeKeys);
        int totalDeleted = 0;
        for (int i = 0; i < keyList.size(); i += 500) {
            List<String> batch = keyList.subList(i, Math.min(i + 500, keyList.size()));
            totalDeleted += deleteCompositeKeysBatch(batch);
        }
        return totalDeleted;
    }

    private int deleteCompositeKeysBatch(List<String> compositeKeys) {
        StringBuilder sql = new StringBuilder("DELETE FROM tbl_product WHERE ");
        List<Object> params = new ArrayList<>();

        for (int i = 0; i < compositeKeys.size(); i++) {
            String[] parts = compositeKeys.get(i).split("\\|\\|\\|", 2);
            if (parts.length == 2) {
                if (i > 0) sql.append(" OR ");
                sql.append("(sku = ? AND platform_product_id = ?)");
                params.add(parts[0]);
                params.add(parts[1]);
            }
        }
        return jdbcTemplate.update(sql.toString(), params.toArray());
    }

    private <T> int tempTableUpsert(List<Product> entities, String tableName, String conflictColumns, String updateSet) throws Exception {
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

    private String generateCsvData(List<Product> products) {
        return products.stream()
                .map(product -> CsvFormatter.joinCsvRow(
                        product.getSku(), product.getPlatformProductId(), product.getProductId(),
                        product.getVariationId(), product.getBarcode(), product.getProductName(),
                        product.getProductDescription(), product.getBrand(), product.getModel(),
                        product.getCategoryLevel1(), product.getCategoryLevel2(), product.getCategoryLevel3(),
                        product.getCategoryPath(), product.getColor(), product.getSize(), product.getMaterial(),
                        product.getWeightGram(), product.getDimensions(), product.getCostPrice(),
                        product.getRetailPrice(), product.getOriginalPrice(), product.getPriceRange(),
                        CsvFormatter.formatBoolean(product.getIsActive()), CsvFormatter.formatBoolean(product.getIsFeatured()),
                        CsvFormatter.formatBoolean(product.getIsSeasonal()), CsvFormatter.formatBoolean(product.getIsNewArrival()),
                        CsvFormatter.formatBoolean(product.getIsBestSeller()), product.getPrimaryImageUrl(),
                        product.getImageCount(), product.getSeoTitle(), product.getSeoKeywords(),
                        product.getSkuGroup()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    public int executeBatchUpsert(List<Product> products) {
        if (products == null || products.isEmpty()) {
            return 0;
        }

        final int CHUNK_SIZE = 1000;
        int totalProcessed = 0;

        log.info("📦 Batch upserting {} products in chunks of {}",
                products.size(), CHUNK_SIZE);

        for (int i = 0; i < products.size(); i += CHUNK_SIZE) {
            int end = Math.min(i + CHUNK_SIZE, products.size());
            List<Product> chunk = products.subList(i, end);

            int[] counts = jdbcTemplate.batchUpdate(
                    UPSERT_SQL,
                    chunk.stream().map(this::mapToParams).toList()
            );
            totalProcessed += counts.length;
        }

        log.info("✅ Product batch completed: {} records", totalProcessed);
        return totalProcessed;
    }

    private Object[] mapToParams(Product p) {
        return new Object[]{
                p.getSku(), p.getPlatformProductId(), p.getProductId(), p.getVariationId(), p.getBarcode(),
                p.getProductName(), p.getProductDescription(), p.getBrand(), p.getModel(),
                p.getCategoryLevel1(), p.getCategoryLevel2(), p.getCategoryLevel3(), p.getCategoryPath(),
                p.getColor(), p.getSize(), p.getMaterial(), p.getWeightGram(), p.getDimensions(),
                p.getCostPrice(), p.getRetailPrice(), p.getOriginalPrice(), p.getPriceRange(),
                p.getIsActive(), p.getIsFeatured(), p.getIsSeasonal(), p.getIsNewArrival(),
                p.getIsBestSeller(), p.getPrimaryImageUrl(), p.getImageCount(), p.getSeoTitle(), p.getSeoKeywords(), p.getSkuGroup()
        };
    }

    private RowMapper<Product> productRowMapper() {
        return (rs, rowNum) -> Product.builder()
                .sku(rs.getString("sku"))
                .platformProductId(rs.getString("platform_product_id"))
                .productId(rs.getString("product_id"))
                .variationId(rs.getString("variation_id"))
                .barcode(rs.getString("barcode"))
                .productName(rs.getString("product_name"))
                .productDescription(rs.getString("product_description"))
                .brand(rs.getString("brand"))
                .model(rs.getString("model"))
                .categoryLevel1(rs.getString("category_level_1"))
                .categoryLevel2(rs.getString("category_level_2"))
                .categoryLevel3(rs.getString("category_level_3"))
                .categoryPath(rs.getString("category_path"))
                .color(rs.getString("color"))
                .size(rs.getString("size"))
                .material(rs.getString("material"))
                .weightGram(rs.getInt("weight_gram"))
                .dimensions(rs.getString("dimensions"))
                .costPrice(rs.getDouble("cost_price"))
                .retailPrice(rs.getDouble("retail_price"))
                .originalPrice(rs.getDouble("original_price"))
                .priceRange(rs.getString("price_range"))
                .isActive(rs.getBoolean("is_active"))
                .isFeatured(rs.getBoolean("is_featured"))
                .isSeasonal(rs.getBoolean("is_seasonal"))
                .isNewArrival(rs.getBoolean("is_new_arrival"))
                .isBestSeller(rs.getBoolean("is_best_seller"))
                .primaryImageUrl(rs.getString("primary_image_url"))
                .imageCount(rs.getInt("image_count"))
                .seoTitle(rs.getString("seo_title"))
                .seoKeywords(rs.getString("seo_keywords"))
                .skuGroup(rs.getString("sku_group"))
                .build();
    }
}