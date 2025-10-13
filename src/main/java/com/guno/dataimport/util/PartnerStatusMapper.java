package com.guno.dataimport.util;

import lombok.experimental.UtilityClass;
import java.util.Map;

/**
 * PartnerStatusMapper - Utility for mapping partner status between String and Integer
 * Centralized mapping logic to avoid duplication across mappers
 */
@UtilityClass
public class PartnerStatusMapper {

    // Mapping table: String → Integer ID
    private static final Map<String, Integer> STRING_TO_ID = Map.ofEntries(
            Map.entry("pending", 1),
            Map.entry("picking_up", 2),
            Map.entry("picked_up", 3),
            Map.entry("on_delivery", 4),
            Map.entry("delivered", 5),
            Map.entry("undeliverable", 6),
            Map.entry("returning", 7),
            Map.entry("returned", 8),
            Map.entry("cancelled", 9),
            Map.entry("unknown", 0)
    );

    // Reverse mapping: Integer ID → String name
    private static final Map<Integer, String> ID_TO_NAME = Map.ofEntries(
            Map.entry(0, "Unknown"),
            Map.entry(1, "Pending"),
            Map.entry(2, "Picking Up"),
            Map.entry(3, "Picked Up"),
            Map.entry(4, "On Delivery"),
            Map.entry(5, "Delivered"),
            Map.entry(6, "Undeliverable"),
            Map.entry(7, "Returning"),
            Map.entry(8, "Returned"),
            Map.entry(9, "Cancelled")
    );

    // Stage mapping: Integer ID → Stage
    private static final Map<Integer, String> ID_TO_STAGE = Map.ofEntries(
            Map.entry(0, "Unknown"),
            Map.entry(1, "Processing"),
            Map.entry(2, "Picking"),
            Map.entry(3, "Picked"),
            Map.entry(4, "Shipping"),
            Map.entry(5, "Completed"),
            Map.entry(6, "Failed"),
            Map.entry(7, "Return"),
            Map.entry(8, "Return"),
            Map.entry(9, "Cancelled")
    );

    /**
     * Map partner status string to integer ID
     * @param statusString e.g. "picking_up", "delivered"
     * @return Integer ID (0 if unknown)
     */
    public static Integer mapToId(String statusString) {
        if (statusString == null || statusString.trim().isEmpty()) {
            return 0; // Unknown
        }

        String normalized = statusString.toLowerCase().trim();
        return STRING_TO_ID.getOrDefault(normalized, 0);
    }

    /**
     * Map integer ID to partner status name
     * @param id Integer ID (0-9)
     * @return Display name (e.g. "Delivered")
     */
    public static String mapToName(Integer id) {
        if (id == null) {
            return "Unknown";
        }
        return ID_TO_NAME.getOrDefault(id, "Unknown");
    }

    /**
     * Map integer ID to stage
     * @param id Integer ID (0-9)
     * @return Stage (e.g. "Processing", "Completed")
     */
    public static String mapToStage(Integer id) {
        if (id == null) {
            return "Unknown";
        }
        return ID_TO_STAGE.getOrDefault(id, "Unknown");
    }
}