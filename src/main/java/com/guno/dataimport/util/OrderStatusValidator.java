package com.guno.dataimport.util;

import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.facebook.ChangedLog;
import com.guno.dataimport.dto.platform.facebook.FacebookItemDto;
import lombok.extern.slf4j.Slf4j;

/**
 * OrderStatusValidator - Updated với logic TikTok tiktok_data
 * Version 7.0 - Tối ưu với tiktok_data.return_refund
 */
@Slf4j
public class OrderStatusValidator {

    // ========== PUBLIC API ==========

    public static boolean isDelivered(FacebookOrderDto order, String platform) {
        if (order == null) return false;
        return switch (platform.toUpperCase()) {
            case "FACEBOOK" -> isFacebookDelivered(order);
            case "TIKTOK" -> isTikTokDelivered(order);
            case "SHOPEE" -> isShopeeDelivered(order);
            default -> false;
        };
    }

    public static boolean isCancelled(FacebookOrderDto order, String platform) {
        if (order == null) return false;

        // Nếu đơn hoàn → không phải cancelled
        if (isReturned(order, platform)) return false;

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

    // ========== TIKTOK LOGIC (NEW - Priority tiktok_data) ==========

    private static boolean isTikTokDelivered(FacebookOrderDto order) {
        // Nếu có return_refund → check return_status
        if (order.hasRefundData()) {
            String returnStatus = order.getTiktokData().getReturnRefund().getReturnStatus();
            // Nếu đang/đã hoàn và chưa delivered thì return false
            if (returnStatus != null && !returnStatus.contains("DELIVERED")) {
                return false;
            }
        }

        // Status 3 = Delivered
        if (order.getStatus() != null && order.getStatus() == 3) return true;

        // Partner status
        if (hasPartnerStatus(order, "delivered")) return true;

        // Tracking histories
        if (hasTrackingContains(order, "delivered") ||
                hasTrackingContains(order, "package delivered")) return true;

        return false;
    }

    private static boolean isTikTokCancelled(FacebookOrderDto order) {
        // Status 6 = Cancelled
        if (order.getStatus() != null && order.getStatus() == 6) return true;

        // Partner status
        if (hasPartnerStatus(order, "cancelled")) return true;

        return false;
    }

    private static boolean isTikTokReturned(FacebookOrderDto order) {
        // PRIORITY 1: Check tiktok_data.return_refund (NEW!)
        if (order.hasRefundData()) {
            String returnType = order.getTiktokData().getReturnRefund().getReturnType();
            if (returnType != null &&
                    (returnType.contains("RETURN") || returnType.contains("REFUND"))) {
                return true;
            }
        }

        // PRIORITY 2: Status 4 (RETURNING) hoặc 5 (RETURNED)
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // PRIORITY 3: Items có return_quantity > 0
        if (hasReturnQuantityInItems(order)) return true;

        // PRIORITY 4: Partner is_returned = true
        if (hasPartnerIsReturned(order)) return true;

        // PRIORITY 5: Partner status
        if (hasPartnerStatus(order, "returned") ||
                hasPartnerStatus(order, "returning")) return true;

        return false;
    }

    // ========== FACEBOOK LOGIC (Giữ nguyên logic cũ) ==========

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
        // Status 4 (RETURNING) hoặc 5 (RETURNED)
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // Items có return_quantity > 0
        if (hasReturnQuantityInItems(order)) return true;

        // Partner is_returned
        if (hasPartnerIsReturned(order)) return true;

        // Partner status
        if (hasPartnerStatus(order, "returned") ||
                hasPartnerStatus(order, "returning")) return true;

        // Histories: shipped → cancelled
        if (hasCancelAfterShippedInHistories(order)) return true;

        return false;
    }

    // ========== SHOPEE LOGIC (Giữ logic cũ phức tạp) ==========

    private static boolean isShopeeDelivered(FacebookOrderDto order) {
        boolean everDelivered = hasEverBeenDeliveredShopee(order);

        // Loại trừ: đang hoàn và chưa từng delivered
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5) && !everDelivered) {
            return false;
        }

        if (hasReturnQuantityInItems(order) && !everDelivered) return false;

        // Khẳng định
        if (everDelivered) return true;
        if (order.getStatus() != null && order.getStatus() == 3) return true;
        if (hasPartnerStatus(order, "delivered")) return true;
        if (hasShopeeStatusInHistories(order, "COMPLETED")) return true;

        return false;
    }

    private static boolean isShopeeCancelled(FacebookOrderDto order) {
        if (order.getStatus() != null && order.getStatus() == 6) return true;

        if (order.getHistories() == null) return false;

        for (ChangedLog log : order.getHistories()) {
            if (log.getShopeeStatus() != null &&
                    "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue())) {
                String oldStatus = log.getShopeeStatus().getOldValue();
                // Chỉ cancelled nếu chưa shipped
                if (oldStatus == null ||
                        !(oldStatus.contains("SHIPPED") || oldStatus.contains("IN_TRANSIT"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isShopeeReturned(FacebookOrderDto order) {
        // Status 4 hoặc 5
        if (order.getStatus() != null &&
                (order.getStatus() == 4 || order.getStatus() == 5)) {
            return true;
        }

        // Partner is_returned
        if (hasPartnerIsReturned(order)) return true;

        // Items có return_quantity
        if (hasReturnQuantityInItems(order)) return true;

        // Histories: shipped → cancelled WITH return_fee hoặc negative COD
        if (order.getHistories() != null) {
            boolean hasShippedToCancelled = false;
            boolean hasReturnFee = false;
            boolean hasNegativeCod = false;

            for (ChangedLog log : order.getHistories()) {
                if (log.getShopeeStatus() != null &&
                        "CANCELLED".equalsIgnoreCase(log.getShopeeStatus().getNewValue()) &&
                        "SHIPPED".equalsIgnoreCase(log.getShopeeStatus().getOldValue())) {
                    hasShippedToCancelled = true;
                }
                if (log.getReturnFee() != null &&
                        Boolean.TRUE.equals(log.getReturnFee().getNewValue())) {
                    hasReturnFee = true;
                }
                if (log.getCod() != null && log.getCod().getNewValue() != null &&
                        log.getCod().getNewValue() < 0) {
                    hasNegativeCod = true;
                }
            }

            if (hasShippedToCancelled && (hasReturnFee || hasNegativeCod)) {
                return true;
            }
        }

        // Partner status
        if (hasPartnerStatus(order, "returned")) return true;

        return false;
    }

    // ========== HELPER METHODS ==========

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

    private static boolean hasEverBeenDeliveredShopee(FacebookOrderDto order) {
        if (order.getHistories() == null) return false;

        return order.getHistories().stream()
                .anyMatch(log -> {
                    if (log.getShopeeStatus() != null) {
                        String newStatus = log.getShopeeStatus().getNewValue();
                        return "COMPLETED".equalsIgnoreCase(newStatus) ||
                                "DELIVERED".equalsIgnoreCase(newStatus);
                    }
                    if (log.getStatus() != null && log.getStatus().getNewValue() != null) {
                        String status = log.getStatus().getNewValue().toString();
                        return status.equals("3") || status.contains("delivered");
                    }
                    return false;
                });
    }

    private static boolean hasShopeeStatusInHistories(FacebookOrderDto order, String status) {
        if (order.getHistories() == null) return false;
        return order.getHistories().stream()
                .anyMatch(log -> log.getShopeeStatus() != null &&
                        status.equalsIgnoreCase(log.getShopeeStatus().getNewValue()));
    }
}