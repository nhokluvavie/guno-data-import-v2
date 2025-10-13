package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.facebook.FacebookCustomer;
import com.guno.dataimport.dto.platform.facebook.FacebookItemDto;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.KeyGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FacebookMapper - Maps Facebook API DTOs to internal entities
 * PHASE 2.2: Added seller fields, is_returned logic, KeyGenerator integration
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FacebookMapper {

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    // ================================
    // CUSTOMER MAPPING (FIXED - Match Entity)
    // ================================

    public Customer mapToCustomer(FacebookOrderDto order) {
        if (order == null || order.getCustomer() == null) return null;

        FacebookCustomer fbCustomer = order.getCustomer();
        String customerId = fbCustomer.getCustomerId() != null ? fbCustomer.getCustomerId() : fbCustomer.getId();

        return Customer.builder()
                .customerId(customerId)
                .customerKey(generateKey(customerId))
                .platformCustomerId(customerId)
                .phoneHash(hashPhone(fbCustomer.getPrimaryPhone()))
                .emailHash(hashEmail(fbCustomer.getPrimaryEmail()))
                .gender(fbCustomer.getGender())
                .ageGroup(null)  // Not available from API
                .customerSegment("FACEBOOK")
                .customerTier("STANDARD")
                .acquisitionChannel("FACEBOOK")
                .firstOrderDate(parseDateTime(fbCustomer.getInsertedAt()))
                .lastOrderDate(parseDateTime(fbCustomer.getLastOrderAt()))
                .totalOrders(safeInt(fbCustomer.getOrderCount()))
                .totalSpent(safeDouble(fbCustomer.getPurchasedAmount()))
                .averageOrderValue(calculateAov(fbCustomer))
                .totalItemsPurchased(0)
                .daysSinceFirstOrder(0)
                .daysSinceLastOrder(0)
                .purchaseFrequencyDays(0.0)
                .returnRate(0.0)
                .cancellationRate(0.0)
                .codPreferenceRate(0.0)
                .favoriteCategory(null)
                .favoriteBrand(null)
                .preferredPaymentMethod(null)
                .preferredPlatform("FACEBOOK")
                .primaryShippingProvince(null)
                .shipsToMultipleProvinces(false)
                .loyaltyPoints(safeInt(fbCustomer.getRewardPoint()))
                .referralCount(safeInt(fbCustomer.getCountReferrals()))
                .isReferrer(safeBool(fbCustomer.getIsReferrer()))
                .build();
    }

    // ================================
    // ORDER MAPPING (ALREADY CORRECT)
    // ================================

    public Order mapToOrder(FacebookOrderDto order) {
        if (order == null) return null;

        return Order.builder()
                .orderId(order.getOrderId())
                .customerId(extractCustomerId(order))
                .shopId(null)
                .internalUuid(null)
                .orderCount(1)
                .itemQuantity(calculateTotalQuantity(order))
                .totalItemsInOrder(order.getItems().size())
                .grossRevenue(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .netRevenue(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .shippingFee(safeDouble(order.getShippingFee()))
                .taxAmount(safeDouble(order.getTax()))
                .discountAmount(safeDouble(order.getDiscount()))
                .codAmount(safeDouble(order.getCod()))
                .platformFee(0.0)
                .sellerDiscount(0.0)
                .platformDiscount(safeDouble(order.getDiscount()))
                .originalPrice(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .estimatedShippingFee(safeDouble(order.getShippingFee()))
                .actualShippingFee(safeDouble(order.getShippingFee()))
                .shippingWeightGram(0)
                .daysToShip(0)
                .isDelivered(isDelivered(order))
                .isCancelled(isCancelled(order))
                .isReturned(isOrderReturned(order))  // ✨ NEW - Option A logic
                .isCod(order.isCodOrder())
                .isNewCustomer(false)
                .isRepeatCustomer(false)
                .isBulkOrder(false)
                .isPromotionalOrder(false)
                .isSameDayDelivery(false)
                .orderToShipHours(0)
                .shipToDeliveryHours(0)
                .totalFulfillmentHours(0)
                .customerOrderSequence(0)
                .customerLifetimeOrders(0)
                .customerLifetimeValue(0.0)
                .daysSinceLastOrder(0)
                .promotionImpact(safeDouble(order.getDiscount()))
                .adRevenue(hasAd(order) ? safeDouble(order.getTotalPriceAfterSubDiscount()) : 0.0)
                .organicRevenue(!hasAd(order) ? safeDouble(order.getTotalPriceAfterSubDiscount()) : 0.0)
                .aov(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .shippingCostRatio(calculateShippingRatio(order))
                .createdAt(parseDateTime(order.getCreatedAt()))  // ✅ FIX: LocalDateTime not String
                .rawData(0)
                .platformSpecificData(0)
                .sellerId(extractSellerId(order))        // ✨ NEW
                .sellerName(extractSellerName(order))    // ✨ NEW
                .sellerEmail(extractSellerEmail(order))  // ✨ NEW
                .build();
    }

    /**
     * ✨ NEW - Determine if order has been returned (Option A: partner_status only)
     */
    private boolean isOrderReturned(FacebookOrderDto order) {
        if (order == null || order.getTrackingHistories() == null) {
            return false;
        }

        // Check if any tracking history has partner_status = "returned"
        return order.getTrackingHistories()
                .stream()
                .anyMatch(h -> "returned".equalsIgnoreCase(h.getPartnerStatus()));
    }

    // ================================
    // ORDER ITEM MAPPING
    // ================================

    public List<OrderItem> mapToOrderItems(FacebookOrderDto order) {
        if (order == null || order.getItems() == null) return new ArrayList<>();

        List<OrderItem> items = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (FacebookItemDto item : order.getItems()) {
            int qty = safeInt(item.getQuantity());
            double price = item.getPriceAsDouble();

            items.add(OrderItem.builder()
                    .orderId(order.getOrderId())
                    .sku(getSku(item))
                    .platformProductId("FB_" + item.getId())
                    .quantity(qty)
                    .unitPrice(price)
                    .totalPrice(price * qty)
                    .itemDiscount(safeDouble(item.getTotalDiscount()))
                    .itemSequence(sequence.getAndIncrement())
                    .opId((long) item.getId().hashCode())
                    .build());
        }
        return items;
    }

    // ================================
    // PRODUCT MAPPING
    // ================================

    public List<Product> mapToProducts(FacebookOrderDto order) {
        if (order == null || order.getItems() == null) return new ArrayList<>();

        List<Product> products = new ArrayList<>();
        for (FacebookItemDto item : order.getItems()) {
            products.add(Product.builder()
                    .sku(getSku(item))
                    .platformProductId("FB_" + item.getId())
                    .productId(item.getProductId())
                    .variationId(item.getVariationId())
                    .barcode(getBarcode(item))
                    .productName(getName(item))
                    .color(getFieldValue(item, "Màu"))
                    .size(getFieldValue(item, "Size"))
                    .weightGram(getWeight(item))
                    .retailPrice(item.getPriceAsDouble())
                    .originalPrice(item.getPriceAsDouble())
                    .priceRange(getPriceRange(item.getPriceAsDouble()))
                    .primaryImageUrl(getImageUrl(item))
                    .imageCount(getImageCount(item))
                    .build());
        }
        return products;
    }

    // ================================
    // GEOGRAPHY MAPPING (WITH KEY GENERATOR)
    // ================================

    public GeographyInfo mapToGeographyInfo(FacebookOrderDto order) {
        if (order == null) return null;

        String province = getProvinceName(order);
        String district = getDistrictName(order);

        return GeographyInfo.builder()
                .orderId(order.getOrderId())
                .geographyKey(KeyGenerator.generateGeographyKey(province, district))  // ✨ NEW
                .countryCode("VN")
                .countryName("Vietnam")
                .provinceName(province)
                .districtName(district)
                .isUrban(isUrbanProvince(province))
                .isMetropolitan(isMetroProvince(province))
                .economicTier(getEconomicTier(province))
                .shippingZone(getShippingZone(province))
                .standardDeliveryDays(getDeliveryDays(province))
                .expressDeliveryAvailable(true)
                .build();
    }

    // ================================
    // PAYMENT MAPPING (WITH KEY GENERATOR)
    // ================================

    public PaymentInfo mapToPaymentInfo(FacebookOrderDto order) {
        if (order == null) return null;

        boolean isCod = order.isCodOrder();
        String paymentMethod = isCod ? "COD" : "ONLINE";
        String paymentProvider = "FACEBOOK_PAY";
        String paymentCategory = isCod ? "CASH_ON_DELIVERY" : "DIGITAL_PAYMENT";

        return PaymentInfo.builder()
                .orderId(order.getOrderId())
                .paymentKey(KeyGenerator.generatePaymentKey(paymentMethod, paymentProvider, paymentCategory))  // ✨ NEW
                .paymentMethod(paymentMethod)
                .paymentCategory(paymentCategory)
                .paymentProvider(paymentProvider)
                .isCod(isCod)
                .isPrepaid(!isCod)
                .supportsRefund(true)
                .supportsPartialRefund(true)
                .refundProcessingDays(7)
                .riskLevel("LOW")
                .settlementDays(1)
                .build();
    }

    // ================================
    // SHIPPING MAPPING (WITH KEY GENERATOR)
    // ================================

    public ShippingInfo mapToShippingInfo(FacebookOrderDto order) {
        if (order == null) return null;

        String providerId = "FACEBOOK";
        String serviceType = "STANDARD";

        return ShippingInfo.builder()
                .orderId(order.getOrderId())
                .shippingKey(KeyGenerator.generateShippingKey(providerId, serviceType))  // ✨ NEW
                .providerId(providerId)
                .providerName("Facebook Marketplace")
                .providerType("MARKETPLACE")
                .baseFee(safeDouble(order.getShippingFee()))
                .supportsCod(order.isCodOrder())
                .coverageProvinces(getProvinceName(order))
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(false)
                .providesSmsUpdates(false)
                .averageDeliveryDays(0.0)
                .onTimeDeliveryRate(0.0)
                .successDeliveryRate(0.0)
                .damageRate(0.0)
                .coverageNationwide(false)
                .coverageInternational(false)
                .build();
    }

    // ================================
    // STATUS MAPPING
    // ================================

    public List<Status> mapToStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<Status> statuses = new ArrayList<>();
        Integer currentStatus = order.getStatus();
        String statusName = order.getStatusName();

        if (currentStatus != null) {
            Status status = Status.builder()
                    .statusKey((long) currentStatus)
                    .platform("FACEBOOK")
                    .platformStatusCode(currentStatus.toString())
                    .platformStatusName(statusName != null ? statusName : getStandardStatusName(currentStatus))
                    .standardStatusCode(mapToStandardStatus(currentStatus))
                    .standardStatusName(getStandardStatusName(currentStatus))
                    .statusCategory(getStatusCategory(currentStatus))
                    .build();
            statuses.add(status);
        }

        return statuses;
    }

    // ================================
    // ORDER STATUS MAPPING
    // ================================

    public List<OrderStatus> mapToOrderStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<OrderStatus> orderStatuses = new ArrayList<>();
        Integer currentStatus = order.getStatus();

        if (currentStatus != null) {
            String subStatusId = extractSubStatusId(order);
            Integer partnerStatusId = extractPartnerStatusId(order);

            OrderStatus orderStatus = OrderStatus.builder()
                    .statusKey((long) currentStatus)
                    .orderId(order.getOrderId())
                    .subStatusId(subStatusId)
                    .partnerStatusId(partnerStatusId)
                    .transitionDateKey(getCurrentDateKey())
                    .transitionTimestamp(parseDateTime(order.getCreatedAt()))
                    .durationInPreviousStatusHours(0)
                    .transitionReason("ORDER_CREATED")
                    .transitionTrigger("SYSTEM")
                    .changedBy("FACEBOOK_API")
                    .isOnTimeTransition(true)
                    .isExpectedTransition(true)
                    .historyKey(generateKey("HIST_" + order.getOrderId()))
                    .createdAt(order.getCreatedAt())
                    .build();
            orderStatuses.add(orderStatus);
        }

        return orderStatuses;
    }

    // ================================
    // PROCESSING DATE MAPPING
    // ================================

    public ProcessingDateInfo mapToProcessingDateInfo(FacebookOrderDto order) {
        if (order == null) return null;

        LocalDateTime orderDate = parseDateTime(order.getCreatedAt());
        if (orderDate == null) {
            orderDate = LocalDateTime.now();
        }

        return ProcessingDateInfo.builder()
                .orderId(order.getOrderId())
                .dateKey(generateDateKey(orderDate))
                .fullDate(orderDate.toLocalDate().toString())
                .dayOfWeek(orderDate.getDayOfWeek().getValue())
                .dayOfWeekName(orderDate.getDayOfWeek().name())
                .dayOfMonth(orderDate.getDayOfMonth())
                .dayOfYear(orderDate.getDayOfYear())
                .weekOfYear(orderDate.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear()))
                .monthOfYear(orderDate.getMonthValue())
                .monthName(orderDate.getMonth().name())
                .quarterOfYear((orderDate.getMonthValue() - 1) / 3 + 1)
                .quarterName("Q" + ((orderDate.getMonthValue() - 1) / 3 + 1))
                .year(orderDate.getYear())
                .isWeekend(orderDate.getDayOfWeek().getValue() >= 6)
                .isHoliday(false)
                .fiscalYear(orderDate.getYear())
                .fiscalQuarter((orderDate.getMonthValue() - 1) / 3 + 1)
                .isShoppingSeason(false)
                .isPeakHour(isPeakHour(orderDate))
                .build();
    }

    // ================================
    // HELPER METHODS - SELLER EXTRACTION
    // ================================

    /**
     * ✨ NEW - Extract seller ID from assigning_seller or default
     */
    private String extractSellerId(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getId() != null) {
            return order.getAssigningSeller().getId();
        }
        return "UNKNOWN";
    }

    /**
     * ✨ NEW - Extract seller name from assigning_seller.name (priority) or account_name (fallback)
     */
    private String extractSellerName(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getName() != null) {
            return order.getAssigningSeller().getName();
        }
        if (order.getAccountName() != null && !order.getAccountName().trim().isEmpty()) {
            return order.getAccountName().trim();
        }
        return "UNKNOWN";
    }

    /**
     * ✨ NEW - Extract seller email from assigning_seller or default
     */
    private String extractSellerEmail(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getEmail() != null) {
            return order.getAssigningSeller().getEmail();
        }
        return "UNKNOWN";
    }

    // ================================
    // HELPER METHODS - STATUS & PARTNER STATUS
    // ================================

    private String extractSubStatusId(FacebookOrderDto order) {
        Integer subStatus = order.getSubStatus();
        return subStatus != null ? subStatus.toString() : "0";
    }

    private Integer extractPartnerStatusId(FacebookOrderDto order) {
        if (order.getTrackingHistories() == null || order.getTrackingHistories().isEmpty()) {
            return 0; // Unknown
        }

        String partnerStatus = order.getTrackingHistories().get(0).getPartnerStatus();
        if (partnerStatus == null) return 0;

        return switch (partnerStatus.toLowerCase()) {
            case "pending" -> 1;
            case "picking_up" -> 2;
            case "picked_up" -> 3;
            case "on_delivery" -> 4;
            case "delivered" -> 5;
            case "undeliverable" -> 6;
            case "returning" -> 7;
            case "returned" -> 8;
            case "cancelled" -> 9;
            default -> 0;
        };
    }

    /**
     * Map status code to standard status string (ALL 17 STATUS)
     */
    private String mapToStandardStatus(Integer status) {
        if (status == null) return "UNKNOWN";
        return switch (status) {
            case -1 -> "CANCELLED";
            case 0 -> "DRAFT";
            case 1 -> "PENDING";
            case 2 -> "CONFIRMED";
            case 3 -> "SHIPPING";
            case 4 -> "DELIVERED";
            case 5 -> "REFUNDED";
            case 6 -> "EXCHANGE";
            case 7 -> "FAILED_DELIVERY";
            case 8 -> "PACKED";
            case 9 -> "READY_TO_SHIP";
            case 10 -> "PROCESSING_RETURN";
            case 11 -> "RETURNED_COMPLETED";
            case 12 -> "PARTIAL_SHIPPED";
            case 13 -> "ON_HOLD";
            case 14 -> "PAYMENT_PENDING";
            case 15 -> "PAYMENT_FAILED";
            case 16 -> "PARTIAL_RETURNED";
            default -> "STATUS_" + status;
        };
    }

    /**
     * Get human-readable status name (ALL 17 STATUS)
     */
    private String getStandardStatusName(Integer status) {
        if (status == null) return "UNKNOWN";
        return switch (status) {
            case -1 -> "Đã hủy";
            case 0 -> "Đơn nháp";
            case 1 -> "Chờ xử lý";
            case 2 -> "Đã xác nhận";
            case 3 -> "Đang giao";
            case 4 -> "Đã giao";
            case 5 -> "Đã hoàn tiền";
            case 6 -> "Đổi hàng";
            case 7 -> "Giao thất bại";
            case 8 -> "Đã đóng gói";
            case 9 -> "Sẵn sàng giao";
            case 10 -> "Đang hoàn";
            case 11 -> "Đã hoàn về";
            case 12 -> "Giao một phần";
            case 13 -> "Tạm giữ";
            case 14 -> "Chờ thanh toán";
            case 15 -> "Thanh toán lỗi";
            case 16 -> "Hoàn một phần";
            default -> "STATUS_" + status;
        };
    }

    /**
     * Get status category (ALL 17 STATUS)
     */
    private String getStatusCategory(Integer status) {
        if (status == null) return "UNKNOWN";
        return switch (status) {
            case -1 -> "CANCELLED";
            case 0, 1, 2 -> "PROCESSING";
            case 3, 8, 9, 12 -> "FULFILLMENT";
            case 4 -> "COMPLETED";
            case 5 -> "REFUND";
            case 6 -> "EXCHANGE";
            case 7 -> "FAILED";
            case 10, 11, 16 -> "RETURN";
            case 13 -> "ON_HOLD";
            case 14, 15 -> "PAYMENT";
            default -> "OTHER";
        };
    }

    // ================================
    // HELPER METHODS - GEOGRAPHY
    // ================================

    private String getProvinceName(FacebookOrderDto order) {
        String province = order.getNewProvinceName();
        return province != null && !province.trim().isEmpty() ? province.trim() : "Unknown";
    }

    private String getDistrictName(FacebookOrderDto order) {
        String district = order.getNewDistrictName();
        return district != null && !district.trim().isEmpty() ? district.trim() : "Unknown";
    }

    private boolean isUrbanProvince(String province) {
        return province != null && (
                province.contains("Hà Nội") || province.contains("Hồ Chí Minh") ||
                        province.contains("Đà Nẵng") || province.contains("Hải Phòng")
        );
    }

    private boolean isMetroProvince(String province) {
        return province != null && (province.contains("Hà Nội") || province.contains("Hồ Chí Minh"));
    }

    private String getEconomicTier(String province) {
        if (isMetroProvince(province)) return "TIER_1";
        if (isUrbanProvince(province)) return "TIER_2";
        return "TIER_3";
    }

    private String getShippingZone(String province) {
        if (province == null) return "ZONE_3";
        if (province.contains("Hà Nội") || province.contains("Hồ Chí Minh")) return "ZONE_1";
        if (isUrbanProvince(province)) return "ZONE_2";
        return "ZONE_3";
    }

    private Integer getDeliveryDays(String province) {
        if (isMetroProvince(province)) return 1;
        if (isUrbanProvince(province)) return 2;
        return 3;
    }

    // ================================
    // HELPER METHODS - GENERAL
    // ================================

    private String extractCustomerId(FacebookOrderDto order) {
        if (order.getCustomer() != null) {
            String customerId = order.getCustomer().getCustomerId();
            if (customerId != null) return customerId;
            return order.getCustomer().getId();
        }
        return "UNKNOWN";
    }

    private int calculateTotalQuantity(FacebookOrderDto order) {
        return order.getItems().stream()
                .mapToInt(item -> safeInt(item.getQuantity()))
                .sum();
    }

    private boolean isDelivered(FacebookOrderDto order) {
        Integer status = order.getStatus();
        return status != null && status == 4;
    }

    private boolean isCancelled(FacebookOrderDto order) {
        Integer status = order.getStatus();
        return status != null && status == -1;
    }

    private boolean hasAd(FacebookOrderDto order) {
        String adId = order.getAdId();
        return adId != null && !adId.trim().isEmpty() && !"null".equalsIgnoreCase(adId);
    }

    private double calculateShippingRatio(FacebookOrderDto order) {
        double total = safeDouble(order.getTotalPriceAfterSubDiscount());
        double shipping = safeDouble(order.getShippingFee());
        return total > 0 ? (shipping / total) * 100 : 0.0;
    }

    private double calculateAov(FacebookCustomer customer) {
        int orders = safeInt(customer.getOrderCount());
        double revenue = safeDouble(customer.getPurchasedAmount());
        return orders > 0 ? revenue / orders : 0.0;
    }

    private String getSku(FacebookItemDto item) {
        if (item.getSku() != null && !item.getSku().trim().isEmpty()) {
            return item.getSku().trim();
        }
        return "SKU_" + item.getId();
    }

    private String getBarcode(FacebookItemDto item) {
        return item.getBarcode() != null ? item.getBarcode() : "";
    }

    private String getName(FacebookItemDto item) {
        return item.getName() != null ? item.getName() : "Unknown Product";
    }

    private String getFieldValue(FacebookItemDto item, String fieldName) {
        if (item.getFields() == null) return null;
        return item.getFields().stream()
                .filter(f -> fieldName.equals(f.getName()))
                .map(FacebookItemDto.Field::getValue)
                .findFirst()
                .orElse(null);
    }

    private int getWeight(FacebookItemDto item) {
        return item.getWeight() != null ? item.getWeight() : 0;
    }

    private String getPriceRange(double price) {
        if (price < 100000) return "UNDER_100K";
        if (price < 500000) return "100K_500K";
        if (price < 1000000) return "500K_1M";
        return "OVER_1M";
    }

    private String getImageUrl(FacebookItemDto item) {
        if (item.getImages() != null && !item.getImages().isEmpty()) {
            return item.getImages().get(0).getUrl();
        }
        return null;
    }

    private int getImageCount(FacebookItemDto item) {
        return item.getImages() != null ? item.getImages().size() : 0;
    }

    private LocalDateTime parseDateTime(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDateTime.parse(dateStr, formatter);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    private Integer getCurrentDateKey() {
        LocalDateTime now = LocalDateTime.now();
        return Integer.parseInt(now.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private Long generateDateKey(LocalDateTime dateTime) {
        return Long.parseLong(dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private Long generateKey(String seed) {
        return (long) Math.abs(seed.hashCode());
    }

    private boolean isPeakHour(LocalDateTime dateTime) {
        int hour = dateTime.getHour();
        return (hour >= 10 && hour <= 14) || (hour >= 18 && hour <= 22);
    }

    private int safeInt(Integer value) {
        return value != null ? value : 0;
    }

    private double safeDouble(Long value) {
        return value != null ? value.doubleValue() : 0.0;
    }

    private double safeDouble(Double value) {
        return value != null ? value : 0.0;
    }

    private boolean safeBool(Boolean value) {
        return value != null && value;
    }
}