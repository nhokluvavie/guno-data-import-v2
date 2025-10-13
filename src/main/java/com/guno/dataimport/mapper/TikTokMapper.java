package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.facebook.*;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.PartnerStatusMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TikTok Mapper - Convert TikTok DTOs to Database Entities
 * REUSES: FacebookOrderDto, FacebookItemDto, FacebookCustomer (same JSON structure)
 * PATTERN: Identical to FacebookMapper with TikTok-specific values
 */
@Component
@Slf4j
public class TikTokMapper {
    @Value("${api.tiktok.default-date:}")
    private String defaultDate;

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    // IMPROVED: Better null handling and simplified logic
    public Customer mapToCustomer(FacebookOrderDto order) {
        if (order == null) return null;

        // ✅ FIX: Handle null customer - create default customer
        if (order.getCustomer() == null) {
            return createDefaultCustomer("UNKNOWN_TIKTOK", "TIKTOK");
        }

        FacebookCustomer customer = order.getCustomer();

        // ✅ FIX: Handle null customer ID
        String customerId = customer.getId() != null ? customer.getId() : "UNKNOWN_TIKTOK";

        return Customer.builder()
                .customerId(customerId)
                .customerKey(generateKey(customerId))
                .platformCustomerId(customerId)
                .phoneHash(hashValue(customer.getPrimaryPhone()))
                .emailHash(hashValue(customer.getPrimaryEmail()))
                .gender(normalizeGender(customer.getGender()))
                .customerSegment("TIKTOK")
                .customerTier("STANDARD")
                .acquisitionChannel("TIKTOK")
                .firstOrderDate(parseDateTime(customer.getInsertedAt()))
                .lastOrderDate(parseDateTime(customer.getLastOrderAt()))
                .totalOrders(safeInt(customer.getOrderCount()))
                .totalSpent(customer.getPurchasedAmountAsDouble())
                .averageOrderValue(calculateAOV(customer))
                .daysSinceFirstOrder(calculateDaysSince(customer.getInsertedAt()))
                .daysSinceLastOrder(calculateDaysSince(customer.getLastOrderAt()))
                .returnRate(calculateReturnRate(customer))
                .preferredPlatform("TIKTOK")
                .primaryShippingProvince(getProvinceName(order))
                .loyaltyPoints(safeInt(customer.getRewardPoint()))
                .referralCount(safeInt(customer.getCountReferrals()))
                .isReferrer(safeBool(customer.getIsReferrer()))
                .build();
    }

    public Order mapToOrder(FacebookOrderDto order) {
        if (order == null) return null;

        double totalAmount = order.getTotalAmountAsDouble();
        double discount = safeDouble(order.getDiscount());
        double grossRevenue = totalAmount + discount; // Before discount

        return Order.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomer() != null ? order.getCustomer().getId() : null)
                .shopId("TIKTOK_SHOP")
                .internalUuid("TT_" + order.getOrderId())
                .itemQuantity(calculateTotalQuantity(order.getItems()))
                .totalItemsInOrder(safeSize(order.getItems()))
                .grossRevenue(grossRevenue)
                .netRevenue(totalAmount)
                .shippingFee(safeDouble(order.getShippingFee()))
                .taxAmount(safeDouble(order.getTax()))
                .discountAmount(discount)
                .codAmount(order.isCodOrder() ? totalAmount : 0.0)
                .platformDiscount(discount)
                .originalPrice(grossRevenue)
                .estimatedShippingFee(safeDouble(order.getShippingFee()))
                .actualShippingFee(safeDouble(order.getShippingFee()))
                .shippingWeightGram(calculateTotalWeight(order.getItems()))
                .isDelivered(isStatus(order.getStatus(), 4)) // Delivered
                .isCancelled(isStatus(order.getStatus(), -1)) // Cancelled
                .isCod(order.isCodOrder())
                .isNewCustomer(isNewCustomer(order.getCustomer()))
                .isRepeatCustomer(!isNewCustomer(order.getCustomer()))
                .isPromotionalOrder(discount > 0)
                .customerLifetimeOrders(order.getCustomer() != null ? safeInt(order.getCustomer().getOrderCount()) : 0)
                .customerLifetimeValue(order.getCustomer() != null ? order.getCustomer().getPurchasedAmountAsDouble() : 0.0)
                .adRevenue(hasAd(order.getAdId()) ? totalAmount : 0.0)
                .organicRevenue(hasAd(order.getAdId()) ? 0.0 : totalAmount)
                .aov(totalAmount)
                .createdAt(parseDateTime(order.getCreatedAt()))
                // NEW FIELDS - Seller information
                .sellerId(extractSellerId(order))
                .sellerName(extractSellerName(order))
                .sellerEmail(extractSellerEmail(order))
                .build();
    }

    private Customer createDefaultCustomer(String customerId, String platform) {
        return Customer.builder()
                .customerId(customerId)
                .customerKey(generateKey(customerId))
                .platformCustomerId(customerId)
                .customerSegment(platform)
                .customerTier("STANDARD")
                .acquisitionChannel(platform)
                .totalOrders(0)
                .totalSpent(0.0)
                .averageOrderValue(0.0)
                .preferredPlatform(platform)
                .build();
    }

    // IMPROVED: Simplified with better null handling
    public List<OrderItem> mapToOrderItems(FacebookOrderDto order) {
        if (order == null || order.getItems() == null) return new ArrayList<>();

        List<OrderItem> items = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (FacebookItemDto item : order.getItems()) {
            int qty = safeInt(item.getQuantity(), 1);
            double price = item.getPriceAsDouble();

            items.add(OrderItem.builder()
                    .orderId(order.getOrderId())
                    .sku(getSku(item))
                    .platformProductId("TIKTOK_" + item.getId())
                    .quantity(qty)
                    .unitPrice(price)
                    .totalPrice(price * qty)
                    .itemDiscount(safeDouble(item.getTotalDiscount()))
                    .itemSequence(sequence.getAndIncrement())
                    .build());
        }
        return items;
    }

    // IMPROVED: Better product mapping
    public List<Product> mapToProducts(FacebookOrderDto order) {
        if (order == null || order.getItems() == null) return new ArrayList<>();

        List<Product> products = new ArrayList<>();
        for (FacebookItemDto item : order.getItems()) {
            products.add(Product.builder()
                    .sku(getSku(item))
                    .platformProductId("TIKTOK_" + item.getId())
                    .productId(item.getProductId())
                    .variationId(item.getVariationId())
                    .barcode(getBarcode(item))
                    .productName(getName(item))
                    .color(getFieldValue(item, "Màu"))
                    .size(getFieldValue(item, "Sz"))
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

    // IMPROVED: Simplified geography mapping
    public GeographyInfo mapToGeographyInfo(FacebookOrderDto order) {
        if (order == null) return null;

        String province = getProvinceName(order);
        return GeographyInfo.builder()
                .orderId(order.getOrderId())
                .geographyKey(generateKey(order.getOrderId()))
                .countryCode("VN")
                .countryName("Vietnam")
                .provinceName(province)
                .districtName(getDistrictName(order))
                .isUrban(isUrbanProvince(province))
                .isMetropolitan(isMetroProvince(province))
                .economicTier(getEconomicTier(province))
                .shippingZone(getShippingZone(province))
                .standardDeliveryDays(getDeliveryDays(province))
                .expressDeliveryAvailable(true)
                .build();
    }

    // IMPROVED: Simplified payment mapping
    public PaymentInfo mapToPaymentInfo(FacebookOrderDto order) {
        if (order == null) return null;

        boolean isCod = order.isCodOrder();
        return PaymentInfo.builder()
                .orderId(order.getOrderId())
                .paymentKey(generateKey("PAY_" + order.getOrderId()))
                .paymentMethod(isCod ? "COD" : "ONLINE")
                .paymentCategory(isCod ? "CASH_ON_DELIVERY" : "DIGITAL_PAYMENT")
                .paymentProvider("TIKTOK_PAY")
                .isCod(isCod)
                .isPrepaid(!isCod)
                .supportsRefund(true)
                .supportsPartialRefund(true)
                .refundProcessingDays(7)
                .riskLevel("LOW")
                .settlementDays(1)
                .build();
    }

    /**
     * Map Facebook order to ShippingInfo entity
     */
    public ShippingInfo mapToShippingInfo(FacebookOrderDto order) {
        if (order == null) return null;

        return ShippingInfo.builder()
                .orderId(order.getOrderId())
                .shippingKey(generateKey("SHIP_" + order.getOrderId()))
                // REAL DATA from Facebook
                .baseFee(safeDouble(order.getShippingFee()))
                .supportsCod(order.isCodOrder())
                .coverageProvinces(getProvinceName(order))
                // REQUIRED DEFAULTS (minimal)
                .providerId("TIKTOK")
                .providerName("TIKTOK Marketplace")
                .providerType("MARKETPLACE")
                // NULL/ZERO for unknown data
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(false)
                .providesSmsUpdates(false)
                .averageDeliveryDays((double) 0) // Unknown
                .onTimeDeliveryRate(0.0) // Unknown
                .successDeliveryRate(0.0) // Unknown
                .damageRate(0.0) // Unknown
                .coverageNationwide(false) // Unknown
                .coverageInternational(false) // Unknown
                .build();
    }

    /**
     * Map Facebook order to Status entities (master data)
     */
    public List<Status> mapToStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<Status> statuses = new ArrayList<>();

        // Facebook platform status mapping
        Integer currentStatus = order.getStatus();
        String statusName = order.getStatusName();

        if (currentStatus != null) {
            Status status = Status.builder()
                    .statusKey((long) currentStatus)
                    .platform("TIKTOK")
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

    /**
     * Map Facebook order to OrderStatus entities (history)
     */
    public List<OrderStatus> mapToOrderStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<OrderStatus> orderStatuses = new ArrayList<>();
        Integer currentStatus = order.getStatus();

        if (currentStatus != null) {
            // Extract sub_status_id and partner_status_id
            String subStatusId = extractSubStatusId(order);
            Integer partnerStatusId = extractPartnerStatusId(order);

            OrderStatus orderStatus = OrderStatus.builder()
                    .statusKey((long) currentStatus)
                    .orderId(order.getOrderId())
                    .subStatusId(subStatusId)                    // NEW
                    .partnerStatusId(partnerStatusId)            // NEW
                    .transitionDateKey(getCurrentDateKey())
                    .transitionTimestamp(parseDateTime(order.getCreatedAt()))
                    .durationInPreviousStatusHours(0)
                    .transitionReason("ORDER_CREATED")
                    .transitionTrigger("SYSTEM")
                    .changedBy("TIKTOK_API")
                    .isOnTimeTransition(true)
                    .isExpectedTransition(true)
                    .historyKey(generateKey("HIST_" + order.getOrderId()))
                    .createdAt(order.getCreatedAt())             // NEW
                    .build();
            orderStatuses.add(orderStatus);
        }

        return orderStatuses;
    }

    /**
     * Map Facebook order to OrderStatusDetail entities
     */
    public List<OrderStatusDetail> mapToOrderStatusDetail(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<OrderStatusDetail> details = new ArrayList<>();

        Integer currentStatus = order.getStatus();
        if (currentStatus != null) {
            boolean isCompleted = isStatus(currentStatus, 4); // Delivered
            boolean isCancelled = isStatus(currentStatus, -1); // Cancelled
            boolean isActive = !isCompleted && !isCancelled;

            OrderStatusDetail detail = OrderStatusDetail.builder()
                    .statusKey((long) currentStatus)
                    .orderId(order.getOrderId())
                    .isActiveOrder(isActive)
                    .isCompletedOrder(isCompleted)
                    .isRevenueRecognized(isCompleted)
                    .isRefundable(isCompleted)
                    .isCancellable(isActive)
                    .isTrackable(true)
                    .nextPossibleStatuses(getNextPossibleStatuses(currentStatus))
                    .autoTransitionHours(24)
                    .requiresManualAction(false)
                    .statusColor(getStatusColor(currentStatus))
                    .statusIcon(getStatusIcon(currentStatus))
                    .customerVisible(true)
                    .customerDescription(getCustomerStatusDescription(currentStatus))
                    .averageDurationHours(getAverageDurationHours(currentStatus))
                    .successRate(95.0)
                    .build();
            details.add(detail);
        }

        return details;
    }

    public ProcessingDateInfo mapToProcessingDateInfo(FacebookOrderDto order) {
        if (order == null) return null;

        LocalDateTime createdAt = parseDateTime(order.getCreatedAt());
        if (createdAt == null) createdAt = LocalDateTime.now();

        return ProcessingDateInfo.builder()
                .orderId(order.getOrderId())
                .dateKey(generateKey(createdAt.toString()))
//                .fullDate(createdAt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                .fullDate(defaultDate)
                .dayOfWeek(createdAt.getDayOfWeek().getValue())
                .dayOfWeekName(createdAt.getDayOfWeek().toString())
                .dayOfMonth(createdAt.getDayOfMonth())
                .dayOfYear(createdAt.getDayOfYear())
                .weekOfYear(getWeekOfYear(createdAt))
                .monthOfYear(createdAt.getMonthValue())
                .monthName(createdAt.getMonth().toString())
                .quarterOfYear(getQuarter(createdAt))
                .quarterName("Q" + getQuarter(createdAt))
                .year(createdAt.getYear())
                .isWeekend(isWeekend(createdAt))
                .isHoliday(false) // Default
                .isBusinessDay(!isWeekend(createdAt))
                .fiscalYear(createdAt.getYear())
                .fiscalQuarter(getQuarter(createdAt))
                .isShoppingSeason(isShoppingSeason(createdAt))
                .seasonName(getSeason(createdAt))
                .isPeakHour(isPeakHour(createdAt))
                .build();
    }

    /**
     * Map Facebook order to SubStatus entities (master data)
     */
    public List<SubStatus> mapToSubStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<SubStatus> subStatuses = new ArrayList<>();
        Integer subStatus = order.getSubStatus();

        if (subStatus != null) {
            SubStatus status = SubStatus.builder()
                    .id(subStatus.toString())
                    .subStatusName(mapSubStatusName(subStatus))
                    .build();
            subStatuses.add(status);
        }

        return subStatuses;
    }

    /**
     * Map Facebook order to PartnerStatus entities (master data from tracking)
     * UPDATED: Use Integer ID and stage field
     */
    public List<PartnerStatus> mapToPartnerStatus(FacebookOrderDto order) {
        if (order == null) return new ArrayList<>();

        List<PartnerStatus> partnerStatuses = new ArrayList<>();

        // Extract unique partner statuses from tracking histories
        order.getTrackingHistories().stream()
                .map(FacebookOrderDto.TrackingHistory::getPartnerStatus)
                .filter(ps -> ps != null && !ps.trim().isEmpty())
                .distinct()
                .forEach(ps -> {
                    Integer statusId = PartnerStatusMapper.mapToId(ps);

                    PartnerStatus status = PartnerStatus.builder()
                            .id(statusId)  // CHANGED: Integer instead of String
                            .partnerStatusName(PartnerStatusMapper.mapToName(statusId))
                            .stage(PartnerStatusMapper.mapToStage(statusId))  // NEW field
                            .build();
                    partnerStatuses.add(status);
                });

        // If no tracking history, use default
        if (partnerStatuses.isEmpty()) {
            partnerStatuses.add(PartnerStatus.builder()
                    .id(0)  // Unknown
                    .partnerStatusName("Unknown")
                    .stage("Unknown")
                    .build());
        }

        return partnerStatuses;
    }

    // IMPROVED: Consolidated helper methods with better null/empty handling
    private String getSku(FacebookItemDto item) {
        if (item.getVariationInfo() != null) {
            String displayId = item.getVariationInfo().getDisplayId();
            if (displayId != null && !displayId.trim().isEmpty()) {
                return displayId.trim();
            }
        }
        return "SKU_" + item.getId();
    }

    private String getBarcode(FacebookItemDto item) {
        if (item.getVariationInfo() != null) {
            String barcode = item.getVariationInfo().getBarcode();
            return (barcode != null && !barcode.trim().isEmpty()) ? barcode.trim() : null;
        }
        return null;
    }

    private String getName(FacebookItemDto item) {
        if (item.getVariationInfo() != null) {
            String name = item.getVariationInfo().getName();
            if (name != null && !name.trim().isEmpty()) {
                return name.trim();
            }
        }
        return "Product " + item.getId();
    }

    private String getFieldValue(FacebookItemDto item, String fieldName) {
        if (item.getVariationInfo() == null || item.getVariationInfo().getFields() == null) return null;
        return item.getVariationInfo().getFields().stream()
                .filter(f -> fieldName.equals(f.getName()))
                .map(FacebookItemDto.VariationField::getValue)
                .findFirst().orElse(null);
    }

    private int getWeight(FacebookItemDto item) {
        return item.getVariationInfo() != null && item.getVariationInfo().getWeight() != null ?
                item.getVariationInfo().getWeight() : 0;
    }

    private String getImageUrl(FacebookItemDto item) {
        if (item.getVariationInfo() == null || item.getVariationInfo().getImages() == null) return null;
        List<String> images = item.getVariationInfo().getImages();
        return !images.isEmpty() ? images.get(0) : null;
    }

    private int getImageCount(FacebookItemDto item) {
        if (item.getVariationInfo() == null || item.getVariationInfo().getImages() == null) return 0;
        return item.getVariationInfo().getImages().size();
    }

    private String getProvinceName(FacebookOrderDto order) {
        return order.getData() != null && order.getData().getShippingAddress() != null ?
                order.getData().getShippingAddress().getProvinceName() : null;
    }

    private String getDistrictName(FacebookOrderDto order) {
        return order.getData() != null && order.getData().getShippingAddress() != null ?
                order.getData().getShippingAddress().getDistrictName() : null;
    }

    // Utility methods - IMPROVED: More concise
    private Long generateKey(String input) {
        return input != null ? Math.abs(input.hashCode()) % 1000000000L : 0L;
    }

    private String hashValue(String value) {
        return value != null ? "HASH_" + Math.abs(value.hashCode()) : null;
    }

    private String normalizeGender(String gender) {
        if (gender == null) return "unknown";
        String lower = gender.toLowerCase();
        return (lower.equals("male") || lower.equals("female")) ? lower : "unknown";
    }

    private int safeInt(Integer value) {
        return value != null ? value : 0;
    }

    private int safeInt(Integer value, int defaultValue) {
        return value != null ? value : defaultValue;
    }

    private double safeDouble(Long value) {
        return value != null ? value.doubleValue() : 0.0;
    }

    private boolean safeBool(Boolean value) {
        return value != null ? value : false;
    }

    private int safeSize(List<?> list) {
        return list != null ? list.size() : 0;
    }

    private boolean isStatus(Integer status, int target) {
        return status != null && status == target;
    }

    private boolean hasAd(String adId) {
        return adId != null && !adId.trim().isEmpty();
    }

    private boolean isNewCustomer(FacebookCustomer customer) {
        return customer == null || safeInt(customer.getOrderCount()) <= 1;
    }

    private double calculateAOV(FacebookCustomer customer) {
        int orders = safeInt(customer.getOrderCount());
        return orders > 0 ? customer.getPurchasedAmountAsDouble() / orders : 0.0;
    }

    private double calculateReturnRate(FacebookCustomer customer) {
        int total = safeInt(customer.getOrderCount());
        int returned = safeInt(customer.getReturnedOrderCount());
        return total > 0 ? (double) returned / total * 100 : 0.0;
    }

    private int calculateDaysSince(String dateStr) {
        LocalDateTime date = parseDateTime(dateStr);
        return date != null ? (int) java.time.Duration.between(date, LocalDateTime.now()).toDays() : 0;
    }

    private int calculateTotalQuantity(List<FacebookItemDto> items) {
        return items != null ? items.stream().mapToInt(item -> safeInt(item.getQuantity(), 1)).sum() : 0;
    }

    private int calculateTotalWeight(List<FacebookItemDto> items) {
        return items != null ? items.stream().mapToInt(item ->
                getWeight(item) * safeInt(item.getQuantity(), 1)).sum() : 0;
    }

    private double calculateRatio(double part, double total) {
        return total > 0 ? (part / total) * 100 : 0.0;
    }

    private boolean isUrbanProvince(String province) {
        if (province == null) return false;
        String p = province.toLowerCase();
        return p.contains("hà nội") || p.contains("hồ chí minh") || p.contains("đà nẵng");
    }

    private boolean isMetroProvince(String province) {
        if (province == null) return false;
        String p = province.toLowerCase();
        return p.contains("hà nội") || p.contains("hồ chí minh");
    }

    private String getEconomicTier(String province) {
        if (province == null) return "TIER_3";
        String p = province.toLowerCase();
        if (p.contains("hà nội") || p.contains("hồ chí minh")) return "TIER_1";
        if (p.contains("đà nẵng") || p.contains("cần thơ")) return "TIER_2";
        return "TIER_3";
    }

    private String getShippingZone(String province) {
        return "ZONE_" + (isMetroProvince(province) ? "1" : isUrbanProvince(province) ? "2" : "3");
    }

    private int getDeliveryDays(String province) {
        if (isMetroProvince(province)) return 1;
        if (isUrbanProvince(province)) return 2;
        return 3;
    }

    private String getPriceRange(double price) {
        if (price < 100000) return "UNDER_100K";
        if (price < 500000) return "100K_500K";
        if (price < 1000000) return "500K_1M";
        return "OVER_1M";
    }

    private LocalDateTime parseDateTime(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) return null;

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDateTime.parse(dateTime.trim(), formatter);
            } catch (DateTimeParseException ignored) {}
        }

        log.warn("Unable to parse datetime: {}", dateTime);
        return null;
    }

    private String mapToStandardStatus(Integer facebookStatus) {
        if (facebookStatus == null) return "UNKNOWN";
        return switch (facebookStatus) {
            case 1 -> "PENDING";
            case 2 -> "CONFIRMED";
            case 3 -> "SHIPPING";
            case 4 -> "DELIVERED";
            case -1 -> "CANCELLED";
            default -> "UNKNOWN";
        };
    }

    private String getStandardStatusName(Integer status) {
        if (status == null) return "Unknown";
        return switch (status) {
            case 1 -> "Pending";
            case 2 -> "Confirmed";
            case 3 -> "Shipping";
            case 4 -> "Delivered";
            case -1 -> "Cancelled";
            default -> "Unknown";
        };
    }

    private String getStatusCategory(Integer status) {
        if (status == null) return "UNKNOWN";
        return switch (status) {
            case 1, 2 -> "PROCESSING";
            case 3 -> "FULFILLMENT";
            case 4 -> "COMPLETED";
            case -1 -> "CANCELLED";
            default -> "UNKNOWN";
        };
    }

    private String getNextPossibleStatuses(Integer currentStatus) {
        if (currentStatus == null) return "";
        return switch (currentStatus) {
            case 1 -> "2,-1"; // Pending -> Confirmed or Cancelled
            case 2 -> "3,-1"; // Confirmed -> Shipping or Cancelled
            case 3 -> "4,-1"; // Shipping -> Delivered or Cancelled
            case 4, -1 -> ""; // Final states
            default -> "";
        };
    }

    private String getStatusColor(Integer status) {
        if (status == null) return "#gray";
        return switch (status) {
            case 1 -> "#orange";   // Pending
            case 2 -> "#blue";     // Confirmed
            case 3 -> "#purple";   // Shipping
            case 4 -> "#green";    // Delivered
            case -1 -> "#red";     // Cancelled
            default -> "#gray";
        };
    }

    private String getStatusIcon(Integer status) {
        if (status == null) return "help-circle";
        return switch (status) {
            case 1 -> "clock";         // Pending
            case 2 -> "check-circle";  // Confirmed
            case 3 -> "truck";         // Shipping
            case 4 -> "package";       // Delivered
            case -1 -> "x-circle";     // Cancelled
            default -> "help-circle";
        };
    }

    private String getCustomerStatusDescription(Integer status) {
        if (status == null) return "Unknown status";
        return switch (status) {
            case 1 -> "Your order is being processed";
            case 2 -> "Your order has been confirmed";
            case 3 -> "Your order is on the way";
            case 4 -> "Your order has been delivered";
            case -1 -> "Your order has been cancelled";
            default -> "Unknown status";
        };
    }

    private double getAverageDurationHours(Integer status) {
        if (status == null) return 0.0;
        return switch (status) {
            case 1 -> 2.0;   // Pending: 2 hours
            case 2 -> 4.0;   // Confirmed: 4 hours
            case 3 -> 24.0;  // Shipping: 24 hours
            case 4 -> 0.0;   // Delivered: final
            case -1 -> 0.0;  // Cancelled: final
            default -> 0.0;
        };
    }

    private Integer getCurrentDateKey() {
        return Integer.valueOf(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private int getWeekOfYear(LocalDateTime date) {
        return date.get(java.time.temporal.WeekFields.ISO.weekOfYear());
    }

    private int getQuarter(LocalDateTime date) {
        return (date.getMonthValue() - 1) / 3 + 1;
    }

    private boolean isWeekend(LocalDateTime date) {
        int dayOfWeek = date.getDayOfWeek().getValue();
        return dayOfWeek == 6 || dayOfWeek == 7;
    }

    private boolean isShoppingSeason(LocalDateTime date) {
        int month = date.getMonthValue();
        return month == 11 || month == 12 || month == 1;
    }

    private String getSeason(LocalDateTime date) {
        int month = date.getMonthValue();
        if (month >= 3 && month <= 5) return "SPRING";
        if (month >= 6 && month <= 8) return "SUMMER";
        if (month >= 9 && month <= 11) return "AUTUMN";
        return "WINTER";
    }

    private boolean isPeakHour(LocalDateTime date) {
        int hour = date.getHour();
        return (hour >= 10 && hour <= 12) || (hour >= 19 && hour <= 21);
    }

    private String getStatusName(Integer status) {
        if (status == null) return "UNKNOWN";
        switch (status) {
            case 0: return "PENDING";
            case 1: return "CONFIRMED";
            case 2: return "PROCESSING";
            case 3: return "SHIPPED";
            case 4: return "DELIVERED";
            case -1: return "CANCELLED";
            default: return "STATUS_" + status;
        }
    }

    /**
     * Extract seller ID from assigning_seller or default
     */
    private String extractSellerId(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getId() != null) {
            return order.getAssigningSeller().getId();
        }
        return "UNKNOWN";
    }

    /**
     * Extract seller name from account_name (primary) or assigning_seller.name (fallback)
     */
    private String extractSellerName(FacebookOrderDto order) {
        // Priority 1: account_name
        if (order.getAccountName() != null && !order.getAccountName().trim().isEmpty()) {
            return order.getAccountName().trim();
        }
        // Priority 2: assigning_seller.name
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getName() != null) {
            return order.getAssigningSeller().getName();
        }
        return "UNKNOWN";
    }

    /**
     * Extract seller email from assigning_seller or default
     */
    private String extractSellerEmail(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getEmail() != null) {
            return order.getAssigningSeller().getEmail();
        }
        return "UNKNOWN";
    }

    /**
     * Extract sub_status_id from order
     */
    private String extractSubStatusId(FacebookOrderDto order) {
        Integer subStatus = order.getSubStatus();
        return subStatus != null ? subStatus.toString() : "0";
    }

    /**
     * Extract partner_status_id from tracking histories (latest)
     * UPDATED: Return Integer ID instead of String
     */
    private Integer extractPartnerStatusId(FacebookOrderDto order) {
        List<FacebookOrderDto.TrackingHistory> histories = order.getTrackingHistories();

        if (histories != null && !histories.isEmpty()) {
            // Get latest partner status
            String partnerStatus = histories.get(0).getPartnerStatus();
            if (partnerStatus != null && !partnerStatus.trim().isEmpty()) {
                return PartnerStatusMapper.mapToId(partnerStatus);  // Use utility
            }
        }

        return 0; // Unknown
    }

    /**
     * Map sub_status integer to name
     */
    private String mapSubStatusName(Integer subStatus) {
        return switch (subStatus) {
            case 0 -> "PENDING";
            case 1 -> "CONFIRMED";
            case 2 -> "PROCESSING";
            case 3 -> "SHIPPING";
            case 4 -> "DELIVERED";
            case -1 -> "CANCELLED";
            default -> "UNKNOWN_" + subStatus;
        };
    }

    /**
     * Check if partner status indicates return
     */
    private boolean isReturnedStatus(String partnerStatus) {
        if (partnerStatus == null) return false;
        String ps = partnerStatus.toLowerCase();
        return ps.contains("return") || ps.contains("returned") ||
                ps.equals("undeliverable") || ps.contains("refused");
    }
}