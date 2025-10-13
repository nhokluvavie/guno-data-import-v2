package com.guno.dataimport.util;

import lombok.experimental.UtilityClass;
import java.util.Objects;

/**
 * KeyGenerator - Utility for generating deterministic unique keys for dimension tables
 * Uses hash-based approach to ensure same input always produces same key
 */
@UtilityClass
public class KeyGenerator {

    /**
     * Generate geography_key from location components
     * @param provinceCode Province code (e.g. "84_VN101")
     * @param districtCode District code (optional)
     * @param wardCode Ward code (optional)
     * @return Unique Long key for geography dimension
     */
    public static Long generateGeographyKey(String provinceCode, String districtCode, String wardCode) {
        // Use Objects.hash for deterministic hashing
        int hash = Objects.hash(
                normalize(provinceCode),
                normalize(districtCode),
                normalize(wardCode)
        );

        // Convert to positive Long to avoid negative keys
        return Math.abs((long) hash);
    }

    /**
     * Generate geography_key with only province and district
     */
    public static Long generateGeographyKey(String provinceCode, String districtCode) {
        return generateGeographyKey(provinceCode, districtCode, null);
    }

    /**
     * Generate geography_key with only province
     */
    public static Long generateGeographyKey(String provinceCode) {
        return generateGeographyKey(provinceCode, null, null);
    }

    /**
     * Generate shipping_key from provider and service information
     * @param providerId Provider ID (e.g. "GHN", "GHTK")
     * @param serviceType Service type (e.g. "EXPRESS", "STANDARD")
     * @param serviceTier Service tier (optional)
     * @return Unique Long key for shipping dimension
     */
    public static Long generateShippingKey(String providerId, String serviceType, String serviceTier) {
        int hash = Objects.hash(
                normalize(providerId),
                normalize(serviceType),
                normalize(serviceTier)
        );

        return Math.abs((long) hash);
    }

    /**
     * Generate shipping_key without tier
     */
    public static Long generateShippingKey(String providerId, String serviceType) {
        return generateShippingKey(providerId, serviceType, null);
    }

    /**
     * Generate payment_key from payment method and provider
     * @param paymentMethod Payment method (e.g. "COD", "BANK_TRANSFER")
     * @param paymentProvider Payment provider (e.g. "MOMO", "ZALOPAY")
     * @param paymentCategory Payment category (optional)
     * @return Unique Long key for payment dimension
     */
    public static Long generatePaymentKey(String paymentMethod, String paymentProvider, String paymentCategory) {
        int hash = Objects.hash(
                normalize(paymentMethod),
                normalize(paymentProvider),
                normalize(paymentCategory)
        );

        return Math.abs((long) hash);
    }

    /**
     * Generate payment_key without category
     */
    public static Long generatePaymentKey(String paymentMethod, String paymentProvider) {
        return generatePaymentKey(paymentMethod, paymentProvider, null);
    }

    /**
     * Generate payment_key with only method
     */
    public static Long generatePaymentKey(String paymentMethod) {
        return generatePaymentKey(paymentMethod, null, null);
    }

    /**
     * Normalize string for consistent hashing
     * - Trim whitespace
     * - Convert to lowercase
     * - Handle null values
     */
    private static String normalize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }
        return value.trim().toLowerCase();
    }

    /**
     * Generate a simple sequential key when deterministic hash is not needed
     * Use timestamp-based key for uniqueness
     */
    public static Long generateSequentialKey() {
        return System.currentTimeMillis();
    }
}