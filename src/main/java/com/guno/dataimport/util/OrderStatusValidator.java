package com.guno.dataimport.util;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.shopee.ShopeeOrderDto;
import com.guno.dataimport.dto.platform.facebook.ChangedLog;
import lombok.extern.slf4j.Slf4j;

/**
 * OrderStatusValidator - Updated với logic Shopee mới
 * Version 8.0 - Shopee Logic với ShopeeOrderDto
 *
 * CHANGELOG:
 * v8.0 - Shopee: Sử dụng ShopeeOrderDto từ package riêng
 * v7.1 - TikTok: Bug Fix isTikTokDelivered không check return_status
 * v7.0 - TikTok: Priority tiktok_data cho return logic
 */
@Slf4j
public class OrderStatusValidator {

    // ========== PUBLIC API ==========

    public static boolean isDelivered(Object order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookDelivered((FacebookOrderDto) order);
            case "TIKTOK" -> isTikTokDelivered((FacebookOrderDto) order);
            case "SHOPEE" -> isShopeeDelivered(order);
            default -> false;
        };
    }

    public static boolean isCancelled(Object order, String platform) {
        if (order == null) return false;

        // Nếu đơn hoàn → không phải cancelled
        if (isReturned(order, platform)) return false;

        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookCancelled((FacebookOrderDto) order);
            case "TIKTOK" -> isTikTokCancelled((FacebookOrderDto) order);
            case "SHOPEE" -> isShopeeCancelled(order);
            default -> false;
        };
    }

    public static boolean isReturned(Object order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookReturned((FacebookOrderDto) order);
            case "TIKTOK" -> isTikTokReturned((FacebookOrderDto) order);
            case "SHOPEE" -> isShopeeReturned(order);
            default -> false;
        };
    }

    // ========== TIKTOK LOGIC (FIXED - Priority tiktok_data) ==========

    private static boolean isTikTokDelivered(FacebookOrderDto order) {
        if (order.getStatus() != null && order.getStatus() == 3) return true;
        if (hasPartnerStatus(order, "delivered")) return true;
        if (hasTrackingContains(order, "delivered") ||
                hasTrackingContains(order, "package delivered")) return true;
        return false;
    }

    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        if (order.getStatus() != null && order.getStatus() == 6) return true;
        if (hasPartnerStatus(order, "cancelled")) return true;
        return false;
    }

    private static boolean isTikTokReturned(FacebookOrderDto order) {
        if (order.hasRefundData()) {
            String returnType = order.getTiktokData().getReturnRefund().getReturnType();
            if (returnType != null &&
                    (returnType.contains("RETURN") || returnType.contains("REFUND"))) {
                return true;
            }
        }

        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        if (hasReturnQuantityInItems(order)) return true;
        if (hasPartnerIsReturned(order)) return true;

        if (hasPartnerStatus(order, "returned") ||
                hasPartnerStatus(order, "returning")) return true;

        return false;
    }

    // ========== FACEBOOK LOGIC ==========

    private static boolean isFacebookDelivered(FacebookOrderDto order) {
        if (order.getStatus() != null && order.getStatus() == 3) return true;
        if (hasPartnerStatus(order, "delivered")) return true;
        if (hasTrackingContains(order, "delivered")) return true;
        return false;
    }

    private static boolean isFacebookCancelled(FacebookOrderDto order) {
        if (order.getStatus() != null && order.getStatus() == 6) return true;
        if (hasPartnerStatus(order, "cancelled")) return true;
        return false;
    }

    private static boolean isFacebookReturned(FacebookOrderDto order) {
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        if (hasReturnQuantityInItems(order)) return true;
        if (hasPartnerIsReturned(order)) return true;

        if (hasPartnerStatus(order, "returned") ||
                hasPartnerStatus(order, "returning")) return true;

        if (hasCancelAfterShippedInHistories(order)) return true;

        return false;
    }

    // ========== SHOPEE LOGIC (NEW - Using ShopeeOrderDto) ==========

    /**
     * Shopee Delivered Logic:
     * - order_status = "COMPLETED" OR "TO_CONFIRM_RECEIVE"
     * - Không phụ thuộc vào return status
     */
    private static boolean isShopeeDelivered(Object orderObj) {
        if (!(orderObj instanceof ShopeeOrderDto)) return false;

        ShopeeOrderDto order = (ShopeeOrderDto) orderObj;
        if (!order.hasOrderDetail()) return false;

        String orderStatus = order.getShopeeOrderStatus();
        if (orderStatus == null) return false;

        return "COMPLETED".equalsIgnoreCase(orderStatus) ||
                "TO_CONFIRM_RECEIVE".equalsIgnoreCase(orderStatus);
    }

    /**
     * Shopee Cancelled Logic:
     * - order_status IN ("CANCELLED", "IN_CANCEL", "INVALID")
     * - Return ưu tiên hơn cancelled (checked in isCancelled())
     */
    private static boolean isShopeeCancelled(Object orderObj) {
        if (!(orderObj instanceof ShopeeOrderDto)) return false;

        ShopeeOrderDto order = (ShopeeOrderDto) orderObj;
        if (!order.hasOrderDetail()) return false;

        String orderStatus = order.getShopeeOrderStatus();
        if (orderStatus == null) return false;

        return "CANCELLED".equalsIgnoreCase(orderStatus) ||
                "IN_CANCEL".equalsIgnoreCase(orderStatus) ||
                "INVALID".equalsIgnoreCase(orderStatus);
    }

    /**
     * Shopee Returned Logic:
     * - Có shopee_data.order_return object
     * - order_return.status != null
     */
    private static boolean isShopeeReturned(Object orderObj) {
        if (!(orderObj instanceof ShopeeOrderDto)) return false;

        ShopeeOrderDto order = (ShopeeOrderDto) orderObj;
        return order.hasOrderReturn();
    }

    // ========== HELPER METHODS (SHARED) ==========

    private static boolean hasPartnerStatus(FacebookOrderDto order, String status) {
        if (order.getTrackingHistories() == null) return false;
        return order.getTrackingHistories().stream()
                .anyMatch(h -> status.equalsIgnoreCase(h.getPartnerStatus()));
    }

    private static boolean hasTrackingContains(FacebookOrderDto order, String keyword) {
        if (order.getTrackingHistories() == null) return false;
        return order.getTrackingHistories().stream()
                .anyMatch(h -> h.getStatus() != null &&
                        h.getStatus().toLowerCase().contains(keyword.toLowerCase()));
    }

    private static boolean hasReturnQuantityInItems(FacebookOrderDto order) {
        if (order.getItems() == null) return false;
        return order.getItems().stream()
                .anyMatch(item -> {
                    Integer returnQty = item.getReturnQuantity();
                    Integer returningQty = item.getReturningQuantity();
                    return (returnQty != null && returnQty > 0) ||
                            (returningQty != null && returningQty > 0);
                });
    }

    private static boolean hasPartnerIsReturned(FacebookOrderDto order) {
        return order.getPartner() != null &&
                Boolean.TRUE.equals(order.getPartner().getIsReturned());
    }

    private static boolean hasCancelAfterShippedInHistories(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getStatus() != null) {
                String newValue = log.getStatus().getNewValue() != null ?
                        log.getStatus().getNewValue().toString() : null;
                String oldValue = log.getStatus().getOldValue() != null ?
                        log.getStatus().getOldValue().toString() : null;

                boolean isCancelled = newValue != null &&
                        (newValue.contains("CANCEL") || newValue.contains("6"));
                boolean wasShipped = oldValue != null &&
                        (oldValue.contains("SHIPPED") || oldValue.contains("IN_TRANSIT") ||
                                oldValue.contains("2") || oldValue.contains("8"));

                if (isCancelled && wasShipped) return true;
            }
        }
        return false;
    }
}