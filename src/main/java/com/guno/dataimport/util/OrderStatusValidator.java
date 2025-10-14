package com.guno.dataimport.util;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto.TrackingHistory;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto.Partner;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto.ExtendUpdate;
import com.guno.dataimport.dto.platform.facebook.ChangedLog;
import com.guno.dataimport.dto.platform.facebook.FacebookItemDto;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Xác định is_delivered, is_cancelled và is_returned theo platform
 * Version 4.0 - Phase 1: Bổ sung logic is_delivered
 *
 * PHÂN LOẠI 3 LOẠI ĐƠN:
 * 1. ĐƠN HỦY:       is_cancelled = TRUE,  is_returned = FALSE, is_delivered = FALSE
 * 2. ĐƠN HOÀN HỦY:  is_cancelled = FALSE, is_returned = TRUE,  is_delivered = FALSE (chưa giao được, hoàn về)
 * 3. ĐƠN TRẢ HÀNG:  is_cancelled = FALSE, is_returned = TRUE,  is_delivered = TRUE  (đã giao, khách trả lại)
 */
@Slf4j
public class OrderStatusValidator {

    // ========== PUBLIC API ==========

    /**
     * Xác định đơn đã được giao hàng thành công chưa
     */
    public static boolean isDelivered(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookDelivered(order);
            case "TIKTOK" -> isTikTokDelivered(order);
            case "SHOPEE" -> isShopeeDelivered(order);
            default -> false;
        };
    }

    /**
     * Xác định đơn đã bị hủy (chưa giao, không hoàn)
     */
    public static boolean isCancelled(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookCancelled(order);
            case "TIKTOK" -> isTikTokCancelled(order);
            case "SHOPEE" -> isShopeeCancelled(order);
            default -> false;
        };
    }

    /**
     * Xác định đơn hoàn/trả hàng (bao gồm cả hoàn hủy và trả hàng sau giao)
     */
    public static boolean isReturned(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookReturned(order);
            case "TIKTOK" -> isTikTokReturned(order);
            case "SHOPEE" -> isShopeeReturned(order);
            default -> false;
        };
    }

    // ========== FACEBOOK LOGIC ==========

    private static boolean isFacebookDelivered(FacebookOrderDto order) {
        // Priority 1: status = 4 (DELIVERED)
        if (order.getStatus() != null && order.getStatus() == 4) {
            return true;
        }

        // Priority 2: tracking_histories có partner_status = "delivered"
        if (hasPartnerStatus(order, "delivered")) {
            return true;
        }

        // Priority 3: tracking_histories có status chứa từ khóa "delivered"
        if (hasTrackingContains(order, "delivered")) {
            return true;
        }

        // Priority 4: tracking_histories có status chứa "giao hàng thành công"
        if (hasTrackingContains(order, "giao hàng thành công") ||
                hasTrackingContains(order, "package delivered") ||
                hasTrackingContains(order, "delivered successfully")) {
            return true;
        }

        // Priority 5: extend_update có status = "delivered"
        if (hasExtendUpdateStatus(order, "delivered")) {
            return true;
        }

        return false;
    }

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
        // CRITICAL: Nếu đã delivered → KHÔNG PHẢI hoàn hủy (là trả hàng)
        // Logic này được giữ nguyên để phân biệt hoàn hủy vs trả hàng

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

    // ========== SHOPEE LOGIC ==========

    private static boolean isShopeeDelivered(FacebookOrderDto order) {
        // Priority 1: status = 4 (DELIVERED)
        if (order.getStatus() != null && order.getStatus() == 4) {
            return true;
        }

        // Priority 2: partner_status = "delivered"
        if (hasPartnerStatus(order, "delivered")) {
            return true;
        }

        // Priority 3: histories có shopee_status = COMPLETED
        if (hasShopeeStatusInHistories(order, "COMPLETED")) {
            return true;
        }

        // Priority 4: tracking chứa từ khóa delivered
        if (hasTrackingContains(order, "delivered") ||
                hasTrackingContains(order, "giao hàng thành công")) {
            return true;
        }

        return false;
    }

    private static boolean isShopeeCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isShopeeReturned(order)) return false;

        if (order.getHistories() == null) return false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null &&
                    "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue())) {
                String oldStatus = log.getShopeeStatus().getOldValue();
                // Chỉ cancelled nếu chưa shipped
                if (oldStatus == null || !"SHIPPED".equalsIgnoreCase(oldStatus)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isShopeeReturned(FacebookOrderDto order) {
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
            // Shipped → Cancelled = hoàn hàng
            if (log.getShopeeStatus() != null &&
                    "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue()) &&
                    "SHIPPED".equalsIgnoreCase(log.getShopeeStatus().getOldValue())) {
                hasShippedToCancelled = true;
            }
            // Có phí hoàn
            if (log.getReturnFee() != null && Boolean.TRUE.equals(log.getReturnFee().getNewValue())) {
                hasReturnFee = true;
            }
            // COD âm = đã hoàn tiền
            if (log.getCod() != null && log.getCod().getNewValue() != null &&
                    log.getCod().getNewValue() < 0) {
                hasNegativeCod = true;
            }
        }

        // Combinations indicating return
        if (hasShippedToCancelled && (hasReturnFee || hasNegativeCod)) {
            return true;
        }

        // Priority 3: partner_status = "returned"
        boolean hasReturnedStatus = hasPartnerStatus(order, "returned");

        // Priority 4: sub_status = 3
        boolean hasReturnedSubStatus = order.getSubStatus() != null && order.getSubStatus() == 3;

        // Priority 5: status = 4 hoặc 5
        boolean isDeliveredOrRefunded = order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5);

        // Priority 6: note có "nhận hàng hoàn"
        boolean hasReturnNote = hasReturnNoteInHistories(order);

        // Combinations
        if (hasReturnedStatus && hasReturnedSubStatus) return true;
        if (hasReturnedStatus && isDeliveredOrRefunded) return true;
        if (hasReturnedSubStatus && hasReturnNote) return true;

        return false;
    }

    // ========== TIKTOK LOGIC ==========

    private static boolean isTikTokDelivered(FacebookOrderDto order) {
        // Priority 1: status = 4 (DELIVERED)
        if (order.getStatus() != null && order.getStatus() == 4) {
            return true;
        }

        // Priority 2: partner_status = "delivered"
        if (hasPartnerStatus(order, "delivered")) {
            return true;
        }

        // Priority 3: tracking chứa "package delivered" hoặc "delivered successfully"
        if (hasTrackingContains(order, "package delivered") ||
                hasTrackingContains(order, "delivered successfully") ||
                hasTrackingContains(order, "giao hàng thành công")) {
            return true;
        }

        // Priority 4: extend_update có "delivered"
        if (hasExtendUpdateStatus(order, "delivered")) {
            return true;
        }

        return false;
    }

    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isTikTokReturned(order)) return false;

        // status = -1 hoặc 4
        boolean isCancelledStatus = order.getStatus() != null &&
                (order.getStatus() == -1 || order.getStatus() == 4);
        return isCancelledStatus;
    }

    private static boolean isTikTokReturned(FacebookOrderDto order) {
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

    // ========== HELPER METHODS ==========

    /**
     * Kiểm tra partner_status có giá trị cụ thể không
     */
    private static boolean hasPartnerStatus(FacebookOrderDto order, String statusValue) {
        if (order.getTrackingHistories() == null || order.getTrackingHistories().isEmpty()) {
            return false;
        }

        return order.getTrackingHistories().stream()
                .anyMatch(h -> h.getPartnerStatus() != null &&
                        statusValue.equalsIgnoreCase(h.getPartnerStatus()));
    }

    /**
     * Kiểm tra tracking_histories.status có chứa keyword không
     */
    private static boolean hasTrackingContains(FacebookOrderDto order, String keyword) {
        if (order.getTrackingHistories() == null || order.getTrackingHistories().isEmpty()) {
            return false;
        }

        String lowerKeyword = keyword.toLowerCase();
        return order.getTrackingHistories().stream()
                .anyMatch(h -> h.getStatus() != null &&
                        h.getStatus().toLowerCase().contains(lowerKeyword));
    }

    /**
     * Kiểm tra extend_update có status cụ thể không
     */
    private static boolean hasExtendUpdateStatus(FacebookOrderDto order, String statusValue) {
        Partner partner = order.getPartner();
        if (partner == null || partner.getExtendUpdate() == null || partner.getExtendUpdate().isEmpty()) {
            return false;
        }

        return partner.getExtendUpdate().stream()
                .anyMatch(update -> update.getStatus() != null &&
                        statusValue.equalsIgnoreCase(update.getStatus()));
    }

    /**
     * Kiểm tra có return_quantity hoặc returning_quantity > 0 không
     */
    private static boolean hasReturnQuantityInItems(FacebookOrderDto order) {
        List<FacebookItemDto> items = order.getItems();
        if (items == null || items.isEmpty()) return false;

        return items.stream().anyMatch(item ->
                (item.getReturnQuantity() != null && item.getReturnQuantity() > 0) ||
                        (item.getReturningQuantity() != null && item.getReturningQuantity() > 0)
        );
    }

    /**
     * Kiểm tra partner.is_returned = true
     */
    private static boolean hasPartnerIsReturned(FacebookOrderDto order) {
        Partner partner = order.getPartner();
        return partner != null && Boolean.TRUE.equals(partner.getIsReturned());
    }

    /**
     * Kiểm tra histories có shopee_status cụ thể không
     */
    private static boolean hasShopeeStatusInHistories(FacebookOrderDto order, String statusValue) {
        if (order.getHistories() == null) return false;

        return order.getHistories().stream()
                .anyMatch(log -> log.getShopeeStatus() != null &&
                        statusValue.equalsIgnoreCase(log.getShopeeStatus().getNewValue()));
    }

    /**
     * Kiểm tra có chuyển từ SHIPPED/IN_TRANSIT → CANCELLED không
     */
    private static boolean hasCancelAfterShippedInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null) {
                String newValue = log.getShopeeStatus().getNewValue();
                String oldValue = log.getShopeeStatus().getOldValue();

                boolean isCancelled = "CANCELLED".equalsIgnoreCase(newValue) ||
                        "CANCEL".equalsIgnoreCase(newValue);
                boolean wasShipped = "SHIPPED".equalsIgnoreCase(oldValue) ||
                        "IN_TRANSIT".equalsIgnoreCase(oldValue);

                if (isCancelled && wasShipped) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Kiểm tra có COD âm trong histories không (dấu hiệu hoàn tiền)
     */
    private static boolean hasNegativeCodInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        return order.getHistories().stream()
                .anyMatch(log -> log.getCod() != null &&
                        log.getCod().getNewValue() != null &&
                        log.getCod().getNewValue() < 0);
    }

    /**
     * Kiểm tra note có chứa "nhận hàng hoàn" không
     */
    private static boolean hasReturnNoteInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        return order.getHistories().stream()
                .anyMatch(log -> log.getNote() != null &&
                        log.getNote().asText().toLowerCase().contains("nhận hàng hoàn"));
    }
}