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
 * Version 6.0 - Updated với Pancake POS Status mapping mới
 *
 * STATUS MAPPING MỚI:
 * - Status 3 = Delivered (đã giao)
 * - Status 4 = Returning (đang hoàn)
 * - Status 5 = Returned (đã hoàn về)
 * - Status 6 = Canceled (đã hủy)
 *
 * PHÂN LOẠI 3 LOẠI ĐƠN:
 * 1. ĐƠN HỦY:       is_cancelled = TRUE,  is_returned = FALSE, is_delivered = FALSE
 * 2. ĐƠN HOÀN HỦY:  is_cancelled = FALSE, is_returned = TRUE,  is_delivered = FALSE (chưa giao được, hoàn về)
 * 3. ĐƠN TRẢ HÀNG:  is_cancelled = FALSE, is_returned = TRUE,  is_delivered = TRUE  (đã giao, khách trả lại)
 *
 * NGUYÊN TẮC:
 * - TikTok/Shopee: Check lịch sử để phân biệt Hoàn hủy vs Trả hàng
 * - Facebook: Logic đơn giản (không có lịch sử status)
 * - Ưu tiên các dấu hiệu trực tiếp (return_quantity, status_name, return_fee)
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

    // ========== FACEBOOK LOGIC (ORIGINAL - Simple) ==========

    /**
     * Facebook: Xác định đơn đã giao thành công
     * Logic đơn giản, không dựa vào lịch sử histories
     */
    private static boolean isFacebookDelivered(FacebookOrderDto order) {
        // Priority 1: status = 3 (DELIVERED)
        if (order.getStatus() != null && order.getStatus() == 3) {
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

    /**
     * Facebook: Xác định đơn đã bị hủy
     */
    private static boolean isFacebookCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isFacebookReturned(order)) return false;

        // Check 1: status = 6 (CANCELED)
        if (order.getStatus() != null && order.getStatus() == 6) return true;

        // Check 2: partner_status = "cancelled"
        if (hasPartnerStatus(order, "cancelled")) return true;

        return false;
    }

    /**
     * Facebook: Xác định đơn hoàn/trả hàng
     */
    private static boolean isFacebookReturned(FacebookOrderDto order) {
        // Priority 1: status = 4 (RETURNING) hoặc 5 (RETURNED)
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // Priority 2: status_name = "returning"
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName)) {
            return true;
        }

        // Priority 3: return_quantity > 0 hoặc returning_quantity > 0
        if (hasReturnQuantityInItems(order)) {
            return true;
        }

        // Priority 4: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 5: histories có IN_TRANSIT/SHIPPED → CANCEL/CANCELLED
        if (hasCancelAfterShippedInHistories(order)) {
            return true;
        }

        // Priority 6: partner_status = "returned" / "returning"
        if (hasPartnerStatus(order, "returned") || hasPartnerStatus(order, "returning")) {
            return true;
        }

        // Priority 7: tracking có dấu hiệu return
        if (hasTrackingContains(order, "returned to seller") ||
                hasTrackingContains(order, "return package") ||
                hasTrackingContains(order, "being returned")) {
            return true;
        }

        return false;
    }

    // ========== SHOPEE LOGIC (Pattern TikTok) ==========

    /**
     * Shopee: Xác định đơn đã giao thành công
     * Áp dụng pattern TikTok: Check lịch sử trước
     */
    private static boolean isShopeeDelivered(FacebookOrderDto order) {
        // BƯỚC 0: Kiểm tra lịch sử
        boolean everDelivered = hasEverBeenDelivered(order);

        // BƯỚC 1: LOẠI TRỪ
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName) && !everDelivered) {
            return false;
        }

        if (hasReturnQuantityInItems(order) && !everDelivered) {
            return false;
        }

        if ((hasPartnerStatus(order, "returning") || hasPartnerStatus(order, "returned"))
                && !everDelivered) {
            return false;
        }

        if (hasCancelAfterShippedInHistories(order)) {
            return false;
        }

        // BƯỚC 2: KHẲNG ĐỊNH
        if (everDelivered) {
            return true;
        }

        if (order.getStatus() != null && order.getStatus() == 3) {
            return true;
        }

        if (hasPartnerStatus(order, "delivered")) {
            return true;
        }

        if (hasShopeeStatusInHistories(order, "COMPLETED")) {
            return true;
        }

        if (hasTrackingContains(order, "delivered") ||
                hasTrackingContains(order, "giao hàng thành công")) {
            return true;
        }

        return false;
    }

    private static boolean isShopeeCancelled(FacebookOrderDto order) {
        // Nếu là đơn hoàn → không phải cancelled
        if (isShopeeReturned(order)) return false;

        // Check status = 6 (CANCELED)
        if (order.getStatus() != null && order.getStatus() == 6) {
            return true;
        }

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
        // Priority 1: status = 4 (RETURNING) hoặc 5 (RETURNED)
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // Priority 2: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 3: return_quantity > 0 hoặc returning_quantity > 0
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

        // Priority 4: partner_status = "returned"
        boolean hasReturnedStatus = hasPartnerStatus(order, "returned");

        // Priority 5: sub_status = 3
        boolean hasReturnedSubStatus = order.getSubStatus() != null && order.getSubStatus() == 3;

        // Priority 6: note có "nhận hàng hoàn"
        boolean hasReturnNote = hasReturnNoteInHistories(order);

        // Combinations
        if (hasReturnedStatus && hasReturnedSubStatus) return true;
        if (hasReturnedStatus && hasReturnNote) return true;
        if (hasReturnedSubStatus && hasReturnNote) return true;

        return false;
    }

    // ========== TIKTOK LOGIC (Pattern chuẩn) ==========

    /**
     * TikTok: Xác định đơn đã giao thành công
     * Pattern chuẩn: Check lịch sử trước để phân biệt Hoàn hủy vs Trả hàng
     */
    private static boolean isTikTokDelivered(FacebookOrderDto order) {
        // BƯỚC 0: Kiểm tra lịch sử
        boolean everDelivered = hasEverBeenDelivered(order);

        // BƯỚC 1: LOẠI TRỪ
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName) && !everDelivered) {
            return false;
        }

        if (hasReturnQuantityInItems(order) && !everDelivered) {
            return false;
        }

        if ((hasPartnerStatus(order, "returning") || hasPartnerStatus(order, "returned"))
                && !everDelivered) {
            return false;
        }

        if (hasCancelAfterShippedInHistories(order)) {
            return false;
        }

        if ((hasExtendUpdateContains(order, "returned to the seller") ||
                hasExtendUpdateContains(order, "being returned") ||
                hasExtendUpdateContains(order, "return package"))
                && !everDelivered) {
            return false;
        }

        if ((hasExtendUpdateContains(order, "customer has rejected") ||
                hasExtendUpdateContains(order, "package has been rejected") ||
                hasExtendUpdateContains(order, "has rejected the package"))
                && !everDelivered) {
            return false;
        }

        // BƯỚC 2: KHẲNG ĐỊNH
        if (everDelivered) {
            return true;
        }

        if (hasPartnerStatus(order, "delivered")) {
            return true;
        }

        if (hasExtendUpdateContains(order, "package delivered") ||
                hasExtendUpdateContains(order, "delivered successfully") ||
                hasExtendUpdateContains(order, "giao hàng thành công")) {
            return true;
        }

        if (hasTrackingContains(order, "package delivered") ||
                hasTrackingContains(order, "delivered successfully")) {
            return true;
        }

        if (hasShopeeStatusInHistories(order, "COMPLETED")) {
            return true;
        }

        if (order.getStatus() != null && order.getStatus() == 3) {
            return true;
        }

        return false;
    }

    /**
     * TikTok: Xác định đơn hoàn/trả hàng
     */
    private static boolean isTikTokReturned(FacebookOrderDto order) {
        // Priority 1: status = 4 (RETURNING) hoặc 5 (RETURNED)
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // Priority 2: return_quantity > 0 hoặc returning_quantity > 0
        if (hasReturnQuantityInItems(order)) {
            return true;
        }

        // Priority 3: status_name = "returning"
        String statusName = order.getStatusName();
        if ("returning".equalsIgnoreCase(statusName)) {
            return true;
        }

        // Priority 4: histories có return_fee = true
        if (hasReturnFeeInHistories(order)) {
            return true;
        }

        // Priority 5: histories có SHIPPED/IN_TRANSIT → CANCEL/CANCELLED
        if (hasCancelAfterShippedInHistories(order)) {
            return true;
        }

        // Priority 6: partner_status = "returned" hoặc "returning"
        if (hasPartnerStatus(order, "returned") || hasPartnerStatus(order, "returning")) {
            return true;
        }

        // Priority 7: partner.is_returned = true
        if (hasPartnerIsReturned(order)) {
            return true;
        }

        // Priority 8: extend_update có "returned to seller" / "being returned"
        if (hasExtendUpdateContains(order, "returned to the seller") ||
                hasExtendUpdateContains(order, "being returned to the seller") ||
                hasExtendUpdateContains(order, "return package")) {
            return true;
        }

        // Priority 9: extend_update có "rejected"
        if (hasExtendUpdateContains(order, "customer has rejected") ||
                hasExtendUpdateContains(order, "package has been rejected") ||
                hasExtendUpdateContains(order, "has rejected the package")) {
            return true;
        }

        // Priority 10: COD âm trong histories
        if (hasNegativeCodInHistories(order)) {
            return true;
        }

        return false;
    }

    /**
     * TikTok: Xác định đơn bị hủy
     */
    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        // CRITICAL: Nếu là đơn hoàn → không phải cancelled
        if (isTikTokReturned(order)) return false;

        // Check 1: status = 6 (CANCELED)
        if (order.getStatus() != null && order.getStatus() == 6) {
            return true;
        }

        // Check 2: partner_status = "cancelled"
        if (hasPartnerStatus(order, "cancelled")) {
            return true;
        }

        // Check 3: extend_update có "cancelled"
        if (hasExtendUpdateContains(order, "cancelled") ||
                hasExtendUpdateContains(order, "order has been cancelled")) {
            return true;
        }

        // Check 4: tracking_histories có "cancelled"
        if (hasTrackingContains(order, "cancelled") ||
                hasTrackingContains(order, "order cancelled")) {
            return true;
        }

        // Check 5: histories có shopee_status = CANCELLED (nhưng KHÔNG phải từ SHIPPED)
        if (order.getHistories() != null) {
            for (ChangedLog log : order.getHistories()) {
                if (log.getShopeeStatus() != null &&
                        "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue())) {
                    String oldStatus = log.getShopeeStatus().getOldValue();
                    // Chỉ cancelled nếu chưa shipped/in_transit
                    if (oldStatus == null ||
                            (!"SHIPPED".equalsIgnoreCase(oldStatus) &&
                                    !"IN_TRANSIT".equalsIgnoreCase(oldStatus))) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    // ========== HELPER METHODS ==========

    /**
     * Kiểm tra đơn hàng đã TỪNG được giao thành công chưa (dựa vào lịch sử)
     * Dùng để phân biệt: Hoàn hủy vs Trả hàng
     *
     * QUAN TRỌNG: Check cả status 3 và 4
     * - Status 3 = Delivered (giao thành công)
     * - Status 4 = Returning (có thể là trả hàng SAU giao)
     * - Case đặc biệt: Status 2 → 4 (hoàn hủy, bỏ qua status 3)
     *
     * @return true nếu đã từng có status=3 hoặc 4 trong histories
     */
    private static boolean hasEverBeenDelivered(FacebookOrderDto order) {
        if (order.getHistories() == null || order.getHistories().isEmpty()) {
            return false;
        }

        // Check trong histories có status chuyển sang 3 hoặc 4 không
        return order.getHistories().stream()
                .anyMatch(log -> log.getStatus() != null &&
                        log.getStatus().getNewValue() != null &&
                            log.getStatus().getNewValue() == 3);
    }

    /**
     * Kiểm tra partner_status có giá trị cụ thể không
     */
    private static boolean hasPartnerStatus(FacebookOrderDto order, String statusValue) {
        if (order.getTrackingHistories() == null || order.getTrackingHistories().isEmpty()) {
            return false;
        }

        return order.getTrackingHistories().stream()
                .anyMatch(h -> statusValue.equalsIgnoreCase(h.getPartnerStatus()));
    }

    /**
     * Kiểm tra tracking status có chứa keyword không
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
     * Kiểm tra extend_update có status cụ thể không (exact match)
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
     * Kiểm tra extend_update có chứa keyword không (case-insensitive, contains)
     */
    private static boolean hasExtendUpdateContains(FacebookOrderDto order, String keyword) {
        Partner partner = order.getPartner();
        if (partner == null || partner.getExtendUpdate() == null || partner.getExtendUpdate().isEmpty()) {
            return false;
        }

        String lowerKeyword = keyword.toLowerCase();
        return partner.getExtendUpdate().stream()
                .anyMatch(update -> update.getStatus() != null &&
                        update.getStatus().toLowerCase().contains(lowerKeyword));
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
     * Kiểm tra histories có return_fee = true không
     */
    private static boolean hasReturnFeeInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        return order.getHistories().stream()
                .anyMatch(log -> log.getReturnFee() != null &&
                        Boolean.TRUE.equals(log.getReturnFee().getNewValue()));
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