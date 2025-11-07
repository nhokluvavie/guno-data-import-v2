package com.guno.dataimport.util;

/**
 * GeographyHelper - Common geography logic for all mappers
 */
public class GeographyHelper {

    public static boolean isUrbanProvince(String province) {
        return province != null && (
                province.contains("Hà Nội") || province.contains("Hồ Chí Minh") ||
                        province.contains("Đà Nẵng") || province.contains("Hải Phòng")
        );
    }

    public static boolean isMetroProvince(String province) {
        return province != null && (
                province.contains("Hà Nội") || province.contains("Hồ Chí Minh")
        );
    }

    public static String getEconomicTier(String province) {
        if (isMetroProvince(province)) return "TIER_1";
        if (isUrbanProvince(province)) return "TIER_2";
        return "TIER_3";
    }

    public static String getShippingZone(String province) {
        if (province == null) return "ZONE_3";
        if (province.contains("Hà Nội") || province.contains("Hồ Chí Minh")) return "ZONE_1";
        if (isUrbanProvince(province)) return "ZONE_2";
        return "ZONE_3";
    }

    public static Integer getDeliveryDays(String province) {
        if (isMetroProvince(province)) return 1;
        if (isUrbanProvince(province)) return 2;
        return 3;
    }
}