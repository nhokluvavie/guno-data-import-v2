package com.guno.dataimport.util;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.facebook.ChangedLog;
import lombok.extern.slf4j.Slf4j;

/**
 * Xác định is_cancelled và is_returned theo platform
 * Version 3.0 - Phân biệt HOÀN HỦY (chưa nhận) vs TRẢ HÀNG (đã nhận)
 *
 * HOÀN HỦY: Khách CHƯA nhận hàng → is_returned = TRUE, is_delivered = FALSE
 * TRẢ HÀNG: Khách ĐÃ nhận hàng → is_returned = TRUE, is_delivered = TRUE
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
        // Nếu là đơn hoàn → không phải cancelled
        if (isFacebookReturned(order)) return false;

        // Check 1: status = -1
        if (order.getStatus() != null && order.getStatus() == -1) return true;

        // Check 2: partner_status = "cancelled"
        if (hasPartnerStatus(order, "cancelled")) return true;

        // Check 3: histories có shopee_status = CANCELLED
        return hasShopeeStatusInHistories(order, "CANCELLED");
    }

    private static boolean isFacebookReturned(FacebookOrderDto order) {
        // Nếu đã delivered → KHÔNG PHẢI hoàn hủy (là trả hàng)
        if (hasDeliveredInTracking(order)) {
            return false;
        }

        // Priority 1: status_name = "returning"
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName)) {
            return true;
        }

        // Priority 2: return_quantity > 0 hoặc returning_quantity > 0
        if (hasReturnQuantityInItems(order)) {
            return true;
        }

        // Priority 3: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 4: partner_status = "returned"
        boolean hasReturnedStatus = hasPartnerStatus(order, "returned");

        // Priority 5: sub_status = 3
        boolean hasReturnedSubStatus = order.getSubStatus() != null && order.getSubStatus() == 3;

        // Priority 6: status = 4 hoặc 5
        boolean isDeliveredOrRefunded = order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5);

        // Priority 7: note có "nhận hàng hoàn"
        boolean hasReturnNote = hasReturnNoteInHistories(order);

        // Combinations
        if (hasReturnedStatus && hasReturnedSubStatus) return true;
        if (hasReturnedStatus && isDeliveredOrRefunded) return true;
        if (hasReturnedSubStatus && hasReturnNote) return true;

        return false;
    }

    // ========== TIKTOK ==========

    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isTikTokReturned(order)) return false;

        // status = -1 hoặc 4
        boolean isCancelledStatus = order.getStatus() != null &&
                (order.getStatus() == -1 || order.getStatus() == 4);
        return isCancelledStatus;
    }

    private static boolean isTikTokReturned(FacebookOrderDto order) {
        // CRITICAL: Nếu đã delivered → KHÔNG PHẢI hoàn hủy (là trả hàng)
        if (hasDeliveredInTracking(order)) {
            return false;
        }

        // Priority 1: status_name = "returning"
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName)) {
            return true;
        }

        // Priority 2: return_quantity > 0 hoặc returning_quantity > 0
        if (hasReturnQuantityInItems(order)) {
            return true;
        }

        // Priority 3: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 4: histories có IN_TRANSIT/SHIPPED → CANCEL/CANCELLED
        if (hasCancelAfterShippedInHistories(order)) {
            return true;
        }

        // Priority 5: status = 5 (REFUNDED) + dấu hiệu khác
        boolean isRefunded = order.getStatus() != null && order.getStatus() == 5;
        if (isRefunded) {
            boolean hasNegativeCod = hasNegativeCodInHistories(order);
            boolean hasReturnTracking = hasTrackingContains(order, "returned to seller") ||
                    hasTrackingContains(order, "return package") ||
                    hasTrackingContains(order, "being returned");
            boolean hasReturnedPartner = hasPartnerStatus(order, "returned");

            return hasNegativeCod || hasReturnTracking || hasReturnedPartner;
        }

        return false;
    }

    // ========== SHOPEE ==========

    private static boolean isShopeeCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isShopeeReturned(order)) return false;

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
        // CRITICAL: Nếu đã delivered → KHÔNG PHẢI hoàn hủy (là trả hàng)
        if (hasDeliveredInTracking(order)) {
            return false;
        }

        // Priority 1: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 2: return_quantity > 0 hoặc returning_quantity > 0
        if (hasReturnQuantityInItems(order)) {
            return true;
        }

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

    private static boolean hasReturnQuantityInItems(FacebookOrderDto order) {
        if (order.getItems() == null) return false;
        return order.getItems().stream()
                .anyMatch(item -> (item.getReturnQuantity() != null && item.getReturnQuantity() > 0) ||
                        (item.getReturningQuantity() != null && item.getReturningQuantity() > 0));
    }

    private static boolean hasCancelAfterShippedInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        boolean hasShipped = false;
        boolean hasCancelled = false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null) {
                String status = log.getShopeeStatus().getNewValue();
                if ("IN_TRANSIT".equalsIgnoreCase(status) || "SHIPPED".equalsIgnoreCase(status)) {
                    hasShipped = true;
                }
                if (("CANCEL".equalsIgnoreCase(status) || "CANCELLED".equalsIgnoreCase(status))
                        && hasShipped) {
                    hasCancelled = true;
                }
            }
        }

        return hasShipped && hasCancelled;
    }

    private static boolean hasPartnerIsReturned(FacebookOrderDto order) {
        if (order.getPartner() != null && order.getPartner().getIsReturned() != null) {
            return order.getPartner().getIsReturned();
        }
        return false;
    }

    /**
     * CRITICAL METHOD: Kiểm tra đã delivered chưa
     * Phân biệt HOÀN HỦY (chưa nhận) vs TRẢ HÀNG (đã nhận)
     */
    private static boolean hasDeliveredInTracking(FacebookOrderDto order) {
        // Check 1: tracking_histories có "delivered"
        if (order.getTrackingHistories() != null) {
            boolean hasDeliveredPartnerStatus = order.getTrackingHistories().stream()
                    .anyMatch(h -> "delivered".equalsIgnoreCase(h.getPartnerStatus()));
            if (hasDeliveredPartnerStatus) return true;

            boolean hasDeliveredInStatus = order.getTrackingHistories().stream()
                    .anyMatch(h -> h.getStatus() != null &&
                            (h.getStatus().toLowerCase().contains("giao hàng thành công") ||
                                    h.getStatus().toLowerCase().contains("delivered successfully") ||
                                    h.getStatus().toLowerCase().contains("package delivered")));
            if (hasDeliveredInStatus) return true;
        }

        // Check 2: extend_update có "delivered"
        // TODO: Cần parse extend_update nếu có trong DTO

        return false;
    }
}