package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Product Repository - JDBC operations for Product entity
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
            primary_image_url, image_count, seo_title, seo_keywords
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (sku, platform_product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_description = EXCLUDED.product_description,
            brand = EXCLUDED.brand,
            retail_price = EXCLUDED.retail_price,
            original_price = EXCLUDED.original_price,
            is_active = EXCLUDED.is_active,
            primary_image_url = EXCLUDED.primary_image_url,
            image_count = EXCLUDED.image_count
        """;

    // Bulk upsert products
    public int bulkUpsert(List<Product> products) {
        if (products == null || products.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} products", products.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, products.stream()
                .map(this::mapProductToParams)
                .toList()
        ).length;
    }

    // Find existing products by composite keys
    public Map<String, Product> findByKeys(Set<String> compositeKeys) {
        if (compositeKeys == null || compositeKeys.isEmpty()) {
            return Map.of();
        }

        // Split composite keys (sku_platformProductId)
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

        List<Product> products = jdbcTemplate.query(sql.toString(), productRowMapper(),
                params.toArray());

        return products.stream()
                .collect(java.util.stream.Collectors.toMap(
                        product -> product.getSku() + "_" + product.getPlatformProductId(),
                        product -> product
                ));
    }

    // Find by SKU
    public List<Product> findBySku(String sku) {
        String sql = "SELECT * FROM tbl_product WHERE sku = ?";
        return jdbcTemplate.query(sql, productRowMapper(), sku);
    }

    // Check if product exists
    public boolean exists(String sku, String platformProductId) {
        String sql = "SELECT 1 FROM tbl_product WHERE sku = ? AND platform_product_id = ?";
        return !jdbcTemplate.queryForList(sql, sku, platformProductId).isEmpty();
    }

    // Get total product count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_product", Long.class);
    }

    // Helper methods
    private Object[] mapProductToParams(Product product) {
        return new Object[]{
                product.getSku(),
                product.getPlatformProductId(),
                product.getProductId(),
                product.getVariationId(),
                product.getBarcode(),
                product.getProductName(),
                product.getProductDescription(),
                product.getBrand(),
                product.getModel(),
                product.getCategoryLevel1(),
                product.getCategoryLevel2(),
                product.getCategoryLevel3(),
                product.getCategoryPath(),
                product.getColor(),
                product.getSize(),
                product.getMaterial(),
                product.getWeightGram(),
                product.getDimensions(),
                product.getCostPrice(),
                product.getRetailPrice(),
                product.getOriginalPrice(),
                product.getPriceRange(),
                product.getIsActive(),
                product.getIsFeatured(),
                product.getIsSeasonal(),
                product.getIsNewArrival(),
                product.getIsBestSeller(),
                product.getPrimaryImageUrl(),
                product.getImageCount(),
                product.getSeoTitle(),
                product.getSeoKeywords()
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
                .build();
    }
}