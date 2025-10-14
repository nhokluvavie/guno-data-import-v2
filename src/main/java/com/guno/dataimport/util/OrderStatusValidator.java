package com.guno.dataimport.util;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.facebook.ChangedLog;
import lombok.extern.slf4j.Slf4j;

/**
 * Xác định is_cancelled và is_returned theo platform
 * Version 2.0 - Sử dụng histories từ JSON
 */
@Slf4j
public class OrderStatusValidator {

    // ========== PUBLIC API ==========

    public static boolean isCancelled(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookCancelled(order);
            case "TIKTOK" -> isTikTokCancelled(order);
            case "SHOPEE" -> isShopeeCancelled(order);
            default -> false;
        };
    }

    public static boolean isReturned(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookReturned(order);
            case "TIKTOK" -> isTikTokReturned(order);
            case "SHOPEE" -> isShopeeReturned(order);
            default -> false;
        };
    }

    // ========== FACEBOOK ==========

    private static boolean isFacebookCancelled(FacebookOrderDto order) {
        // Check 1: status = -1
        if (order.getStatus() != null && order.getStatus() == -1) return true;

        // Check 2: partner_status = "cancelled"
        if (hasPartnerStatus(order, "cancelled")) return true;

        // Check 3: histories có shopee_status = CANCELLED
        return hasShopeeStatusInHistories(order, "CANCELLED");
    }

    private static boolean isFacebookReturned(FacebookOrderDto order) {
        boolean hasReturnedStatus = hasPartnerStatus(order, "returned");
        boolean hasReturnedSubStatus = order.getSubStatus() != null && order.getSubStatus() == 3;
        boolean isDeliveredOrRefunded = order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5);
        boolean hasReturnNote = hasReturnNoteInHistories(order);

        return (hasReturnedStatus && hasReturnedSubStatus) ||
                (hasReturnedStatus && isDeliveredOrRefunded) ||
                (hasReturnedSubStatus && hasReturnNote);
    }

    // ========== TIKTOK ==========

    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        boolean isCancelledStatus = order.getStatus() != null &&
                (order.getStatus() == -1 || order.getStatus() == 4);
        boolean hasNoReturn = !hasTrackingContains(order, "returned") &&
                !hasTrackingContains(order, "returning");
        return isCancelledStatus && hasNoReturn;
    }

    private static boolean isTikTokReturned(FacebookOrderDto order) {
        boolean isRefunded = order.getStatus() != null && order.getStatus() == 5;
        boolean hasNegativeCod = hasNegativeCodInHistories(order);
        boolean hasReturnTracking = hasTrackingContains(order, "returned to seller") ||
                hasTrackingContains(order, "return package") ||
                hasTrackingContains(order, "being returned");
        boolean hasReturnedPartner = hasPartnerStatus(order, "returned");

        return isRefunded && (hasNegativeCod || hasReturnTracking || hasReturnedPartner);
    }

    // ========== SHOPEE ==========

    private static boolean isShopeeCancelled(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null &&
                    "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue())) {
                String oldStatus = log.getShopeeStatus().getOldValue();
                if (oldStatus == null || !"SHIPPED".equalsIgnoreCase(oldStatus)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isShopeeReturned(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        boolean hasShippedToCancelled = false;
        boolean hasReturnFee = false;
        boolean hasNegativeCod = false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null &&
                    "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue()) &&
                    "SHIPPED".equalsIgnoreCase(log.getShopeeStatus().getOldValue())) {
                hasShippedToCancelled = true;
            }
            if (log.getReturnFee() != null && Boolean.TRUE.equals(log.getReturnFee().getNewValue())) {
                hasReturnFee = true;
            }
            if (log.getCod() != null && log.getCod().getNewValue() != null &&
                    log.getCod().getNewValue() < 0) {
                hasNegativeCod = true;
            }
        }

        return hasShippedToCancelled && (hasReturnFee || hasNegativeCod);
    }

    // ========== HELPERS ==========

    private static boolean hasPartnerStatus(FacebookOrderDto order, String status) {
        if (order.getTrackingHistories() == null) return false;
        return order.getTrackingHistories().stream()
                .anyMatch(h -> status.equalsIgnoreCase(h.getPartnerStatus()));
    }

    private static boolean hasTrackingContains(FacebookOrderDto order, String text) {
        if (order.getTrackingHistories() == null) return false;
        return order.getTrackingHistories().stream()
                .anyMatch(h -> h.getStatus() != null &&
                        h.getStatus().toLowerCase().contains(text.toLowerCase()));
    }

    private static boolean hasShopeeStatusInHistories(FacebookOrderDto order, String status) {
        if (order.getHistories() == null) return false;
        return order.getHistories().stream()
                .anyMatch(log -> log.getShopeeStatus() != null &&
                        status.equalsIgnoreCase(log.getShopeeStatus().getNewValue()));
    }

    private static boolean hasReturnNoteInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;
        return order.getHistories().stream()
                .anyMatch(log -> {
                    String noteValue = log.getNoteValue();
                    return noteValue != null && noteValue.toLowerCase().contains("nhận hàng hoàn");
                });
    }

    private static boolean hasNegativeCodInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;
        return order.getHistories().stream()
                .anyMatch(log -> log.getCod() != null &&
                        log.getCod().getNewValue() != null &&
                        log.getCod().getNewValue() < 0);
    }
}