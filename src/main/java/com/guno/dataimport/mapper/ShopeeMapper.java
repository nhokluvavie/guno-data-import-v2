package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.shopee.*;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.GeographyHelper;
import com.guno.dataimport.util.KeyGenerator;
import com.guno.dataimport.util.OrderStatusValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shopee Mapper - CORRECTED VERSION
 * All entity field names verified and corrected
 */
@Component
@Slf4j
public class ShopeeMapper {

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    // Status codes
    private static final Long STATUS_NEW = 0L;
    private static final Long STATUS_CONFIRMED = 1L;
    private static final Long STATUS_SHIPPED = 2L;
    private static final Long STATUS_DELIVERED = 3L;
    private static final Long STATUS_RETURNED = 4L;
    private static final Long STATUS_CANCELED = 6L;
    private static final Long STATUS_PACKAGING = 8L;

    // ================================
    // 1. CUSTOMER MAPPING
    // ================================

    public Customer mapToCustomer(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return null;

        ShopeeOrderDetail detail = order.getOrderDetail();
        ShopeeRecipientAddress address = detail.getRecipientAddress();
        String customerId = extractCustomerId(order);

        return Customer.builder()
                .customerId(customerId)
                .customerKey((long) customerId.hashCode())
                .platformCustomerId(customerId)
                .phoneHash(address != null ? address.getPhone() : "")
                .emailHash("")
                .gender("")
                .ageGroup("")
                .customerSegment("SHOPEE")
                .customerTier("STANDARD")
                .acquisitionChannel("SHOPEE")
                .firstOrderDate(timestampToDateTime(detail.getCreateTime()))
                .lastOrderDate(timestampToDateTime(detail.getUpdateTime()))
                .totalOrders(1)
                .totalSpent(detail.getTotalAmountAsDouble())
                .averageOrderValue(detail.getTotalAmountAsDouble())
                .totalItemsPurchased(detail.getItemCount())
                .daysSinceFirstOrder(0)
                .daysSinceLastOrder(0)
                .purchaseFrequencyDays(0.0)
                .returnRate(0.0)
                .cancellationRate(0.0)
                .codPreferenceRate(detail.isCodOrder() ? 1.0 : 0.0)
                .favoriteCategory("")
                .favoriteBrand("")
                .preferredPaymentMethod(detail.getPaymentMethod())
                .preferredPlatform("SHOPEE")
                .primaryShippingProvince(address != null ? address.getState() : "")
                .shipsToMultipleProvinces(false)
                .loyaltyPoints(0)
                .referralCount(0)
                .isReferrer(false)
                .customerName(address != null ? address.getName() : "")
                .build();
    }

    // ================================
    // 2. ORDER MAPPING - CORRECTED
    // ================================

    public Order mapToOrder(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return null;

        ShopeeOrderDetail detail = order.getOrderDetail();
        ShopeeOrderReturn returnInfo = order.getOrderReturn();

        Double totalAmount = detail.getTotalAmountAsDouble();
        Double shippingFee = detail.getActualShippingFeeAsDouble();

        return Order.builder()
                .orderId(order.getOrderIdSafe())
                .customerId(extractCustomerId(order))
                .shopId("SHOPEE_" + order.getShopId())
                .internalUuid("SHOPEE")
                .orderCount(1)
                .itemQuantity(detail.getItemCount())
                .totalItemsInOrder(detail.getItemCount())
                .grossRevenue(totalAmount)
                .netRevenue(totalAmount)
                .shippingFee(shippingFee)
                .taxAmount(0.0)
                .discountAmount(0.0)
                .codAmount(detail.isCodOrder() ? totalAmount : 0.0)
                // ✅ REMOVED: .finalAmount() - không tồn tại
                // ✅ REMOVED: .isOnlinePayment() - không tồn tại
                .isCod(detail.isCodOrder())
                .platformFee(0.0)
                .sellerDiscount(0.0)
                .platformDiscount(0.0)
                .originalPrice(totalAmount)
                .estimatedShippingFee(safeDouble(detail.getEstimatedShippingFee()))
                .actualShippingFee(shippingFee)
                .shippingWeightGram(safeInt(detail.getOrderChargeableWeightGram()))
                .daysToShip(safeInt(detail.getDaysToShip()))
                .isDelivered(OrderStatusValidator.isDelivered(order, "SHOPEE"))
                .isCancelled(OrderStatusValidator.isCancelled(order, "SHOPEE"))
                .isReturned(OrderStatusValidator.isReturned(order, "SHOPEE"))
                .isNewCustomer(true)
                .isRepeatCustomer(false)
                .isBulkOrder(false)
                .isPromotionalOrder(false)
                .isSameDayDelivery(false)
                .orderToShipHours(0)
                .shipToDeliveryHours(0)
                .totalFulfillmentHours(0)
                .customerOrderSequence(1)
                .customerLifetimeOrders(1)
                .customerLifetimeValue(totalAmount)
                .daysSinceLastOrder(0)
                .promotionImpact(0.0)
                .adRevenue(0.0)
                .organicRevenue(totalAmount)
                .aov(totalAmount)
                .shippingCostRatio(totalAmount > 0 ? shippingFee / totalAmount : 0.0)
                .createdAt(order.getInsertedAt())
                .orderSource("SHOPEE")
                // ✅ FIXED: platformSpecificData phải là Integer
                .platformSpecificData(order.getStatus() != null ? order.getStatus() : 0)
                .sellerId("SHOPEE")
                .sellerName("Shopee")
                .sellerEmail("shopee@shop.com")
                .latestStatus(mapShopeeToLatestStatus(order))
                .isRefunded(returnInfo != null)
                .refundAmount(returnInfo != null ? returnInfo.getRefundAmountAsDouble() : 0.0)
                .refundDate(returnInfo != null && returnInfo.getUpdateTime() != null ?
                        timestampToDateTime(returnInfo.getUpdateTime()).toString() : "")
                .isExchanged(false)
                .cancelReason(detail.getCancelReason())
                .cancelTime(detail.getCancelBy() != null && !detail.getCancelBy().isEmpty() && detail.getUpdateTime() != null ?
                        timestampToDateTime(detail.getUpdateTime()).toString() : "")
                .orderDt(order.getInsertedAt() != null ? order.getInsertedAt().toLocalDate().toString() : "")
                .build();
    }

    // ================================
    // 3. ORDER ITEMS MAPPING - CORRECTED
    // ================================

    public List<OrderItem> mapToOrderItems(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return new ArrayList<>();

        ShopeeOrderDetail detail = order.getOrderDetail();
        if (!detail.hasItems()) return new ArrayList<>();

        List<OrderItem> items = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (ShopeeItem item : detail.getItemList()) {
            int qty = item.getQuantity();
            double price = item.getDiscountedPriceAsDouble();

            items.add(OrderItem.builder()
                    .orderId(order.getOrderIdSafe())
                    .sku(item.getSkuOrDefault())
                    .platformProductId("SHOPEE_" + item.getItemId())
                    .quantity(qty)
                    .unitPrice(price)
                    .totalPrice(price * qty)
                    .itemDiscount(item.getOriginalPriceAsDouble() - item.getDiscountedPriceAsDouble())
                    // ✅ REMOVED: .finalItemPrice() - không tồn tại
                    .promotionType(item.getPromotionType())
                    .promotionCode(item.getPromotionId() != null ? item.getPromotionId().toString() : null)
                    .itemStatus(null)
                    .itemSequence(sequence.getAndIncrement())
                    .opId((long) item.getItemId().hashCode())
                    .build());
        }
        return items;
    }

    // ================================
    // 4. PRODUCTS MAPPING
    // ================================

    public List<Product> mapToProducts(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return new ArrayList<>();

        ShopeeOrderDetail detail = order.getOrderDetail();
        if (!detail.hasItems()) return new ArrayList<>();

        List<Product> products = new ArrayList<>();

        for (ShopeeItem item : detail.getItemList()) {
            String sku = item.getSkuOrDefault();

            products.add(Product.builder()
                    .sku(sku)
                    .platformProductId("SHOPEE_" + item.getItemId())
                    .productId(item.getItemId().toString())
                    .variationId(item.getModelId() != null ? item.getModelId().toString() : "")
                    .barcode("")
                    .productName(item.getItemName())
                    .color(extractColorFromModelName(item.getModelName()))
                    .size(extractSizeFromModelName(item.getModelName()))
                    .weightGram(item.getWeight() != null ? (int)(item.getWeight() * 1000) : 0)
                    .retailPrice(item.getDiscountedPriceAsDouble())
                    .originalPrice(item.getOriginalPriceAsDouble())
                    .priceRange(getPriceRange(item.getDiscountedPriceAsDouble()))
                    .primaryImageUrl(item.getImageInfo() != null ? item.getImageInfo().getImageUrl() : "")
                    .imageCount(item.getImageInfo() != null ? 1 : 0)
                    .skuGroup(extractSkuGroup(sku))
                    .build());
        }
        return products;
    }

    // ================================
    // 5. GEOGRAPHY MAPPING
    // ================================

    public GeographyInfo mapToGeographyInfo(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return null;

        ShopeeOrderDetail detail = order.getOrderDetail();
        ShopeeRecipientAddress address = detail.getRecipientAddress();

        String province = address != null ? address.getState() : "Unknown";
        String district = address != null ? address.getCity() : "Unknown";

        return GeographyInfo.builder()
                .orderId(order.getOrderIdSafe())
                .geographyKey(KeyGenerator.generateGeographyKey(province, district))
                .countryCode("VN")
                .countryName("Vietnam")
                .regionCode("")
                .regionName("")
                .provinceCode("")
                .provinceName(province)
                .provinceType("")
                .districtCode("")
                .districtName(district)
                .districtType("")
                .wardCode("")
                .wardName(address != null ? address.getDistrict() : "")
                .wardType("")
                .isUrban(GeographyHelper.isUrbanProvince(province))
                .isMetropolitan(GeographyHelper.isMetroProvince(province))
                .isCoastal(false)
                .isBorder(false)
                .economicTier(GeographyHelper.getEconomicTier(province))
                .populationDensity("")
                .incomeLevel("")
                .shippingZone(GeographyHelper.getShippingZone(province))
                .deliveryComplexity("")
                .standardDeliveryDays(GeographyHelper.getDeliveryDays(province))
                .expressDeliveryAvailable(true)
                .latitude(0.0)
                .longitude(0.0)
                .build();
    }

    // ================================
    // 6. PAYMENT MAPPING
    // ================================

    public PaymentInfo mapToPaymentInfo(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return null;

        ShopeeOrderDetail detail = order.getOrderDetail();
        boolean isCod = detail.isCodOrder();
        String method = isCod ? "COD" : "ONLINE";
        String provider = isCod ? "CASH" : "SHOPEE_PAY";
        String category = isCod ? "CASH_ON_DELIVERY" : "ONLINE_PAYMENT";

        return PaymentInfo.builder()
                .orderId(order.getOrderIdSafe())
                // ✅ SYNCED WITH TIKTOK/FACEBOOK
                .paymentKey(KeyGenerator.generatePaymentKey(method, provider, category))
                .paymentMethod(method)
                .paymentCategory(category)
                .paymentProvider(provider)
                .isCod(isCod)
                .isPrepaid(!isCod)
                .isInstallment(false)
                .installmentMonths(0)
                .supportsRefund(true)
                .supportsPartialRefund(true)
                .refundProcessingDays(7)
                .riskLevel("LOW")
                .requiresVerification(false)
                .fraudScore(0.0)
                .transactionFeeRate(0.0)
                .processingFee(0.0)
                .paymentProcessingTimeMinutes(0)
                .settlementDays(1)
                .build();
    }

    // ================================
    // 7. SHIPPING MAPPING - CORRECTED
    // ================================

    public ShippingInfo mapToShippingInfo(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return null;

        ShopeeOrderDetail detail = order.getOrderDetail();

        return ShippingInfo.builder()
                .orderId(order.getOrderIdSafe())
                .shippingKey(generateKey(order.getOrderIdSafe() + "_shipping"))
                // ✅ CORRECTED: shippingProvider → providerName
                .providerId("SHOPEE_" + detail.getShippingCarrier())
                .providerName(detail.getShippingCarrier())
                .providerType("THIRD_PARTY")
                .providerTier("STANDARD")
                .serviceType("STANDARD")
                .serviceTier("ECONOMY")
                .deliveryCommitment("")
                .shippingMethod("STANDARD")
                .pickupType("")
                .deliveryType("")
                // ✅ REMOVED: trackingNumber, trackingUrl, shippingStatus - không tồn tại
                .baseFee(detail.getActualShippingFeeAsDouble())
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsCod(detail.isCodOrder())
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(true)
                .providesSmsUpdates(false)
                .averageDeliveryDays((double) safeInt(detail.getDaysToShip()))
                .onTimeDeliveryRate(0.0)
                .successDeliveryRate(0.0)
                .damageRate(0.0)
                .coverageProvinces(detail.getProvince())
                .coverageNationwide(false)
                .coverageInternational(false)
                .build();
    }

    // ================================
    // 8. PROCESSING DATE MAPPING
    // ================================

    public ProcessingDateInfo mapToProcessingDateInfo(ShopeeOrderDto order) {
        if (order == null) return null;

        LocalDateTime orderDate = order.getInsertedAt();
        if (orderDate == null) return null;

        return ProcessingDateInfo.builder()
                .orderId(order.getOrderIdSafe())
                .dateKey(generateDateKey(orderDate))
                .fullDate(orderDate.toLocalDate().toString())
                .dayOfWeek(orderDate.getDayOfWeek().getValue())
                .dayOfWeekName(orderDate.getDayOfWeek().name())
                .weekOfYear(orderDate.get(WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear()))
                .monthOfYear(orderDate.getMonthValue())
                .monthName(orderDate.getMonth().name())
                .quarterOfYear(((orderDate.getMonthValue() - 1) / 3 + 1))
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
    // 9. ORDER STATUS MAPPING - CORRECTED
    // ================================

    public List<OrderStatus> mapToOrderStatus(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return new ArrayList<>();

        List<OrderStatus> orderStatuses = new ArrayList<>();
        Long currentStatus = mapShopeeToLatestStatus(order);

        OrderStatus orderStatus = OrderStatus.builder()
                .statusKey(currentStatus)
                .orderId(order.getOrderIdSafe())
                .subStatusId("0")  // ✅ REQUIRED
                .partnerStatusId(0)  // ✅ REQUIRED
                .transitionDateKey(getCurrentDateKey())
                .transitionTimestamp(order.getInsertedAt())
                // ✅ CORRECTED: statusDuration → durationInPreviousStatusHours (Integer)
                .durationInPreviousStatusHours(0)
                // ✅ REMOVED: previousStatusKey, isCurrentStatus - không tồn tại
                // ✅ CORRECTED: statusReason → transitionReason
                .transitionReason("")
                .transitionTrigger("SYSTEM")
                // ✅ CORRECTED: modifiedBy → changedBy
                .changedBy("SYSTEM")
                .isOnTimeTransition(true)
                .isExpectedTransition(true)
                .historyKey(0L)
                // ✅ CORRECTED: modifiedAt → createdAt (String)
                .createdAt(order.getInsertedAt() != null ? order.getInsertedAt().toString() : "")
                .build();

        orderStatuses.add(orderStatus);
        return orderStatuses;
    }

    // ================================
    // HELPER METHODS - STATUS MAPPING
    // ================================

    private Long mapShopeeToLatestStatus(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) return STATUS_NEW;

        // Priority 1: Return
        if (order.hasOrderReturn()) {
            return STATUS_RETURNED;
        }

        // Priority 2: Order Status
        String orderStatus = order.getShopeeOrderStatus();
        if (orderStatus == null) return STATUS_NEW;

        return switch (orderStatus.toUpperCase()) {
            case "UNPAID" -> STATUS_NEW;
            case "PROCESSED" -> STATUS_CONFIRMED;
            case "READY_TO_SHIP" -> STATUS_PACKAGING;
            case "SHIPPED" -> STATUS_SHIPPED;
            case "TO_CONFIRM_RECEIVE", "COMPLETED" -> STATUS_DELIVERED;
            case "CANCELLED", "IN_CANCEL", "INVALID" -> STATUS_CANCELED;
            case "TO_RETURN" -> STATUS_RETURNED;
            default -> STATUS_NEW;
        };
    }

    // ================================
    // HELPER METHODS - EXTRACTION
    // ================================

    private String extractCustomerId(ShopeeOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return "GUEST_" + order.getOrderIdSafe();
        }

        ShopeeOrderDetail detail = order.getOrderDetail();
        ShopeeRecipientAddress address = detail.getRecipientAddress();

        if (address != null && address.getPhone() != null && !address.getPhone().isEmpty()) {
            return "SHOPEE_" + address.getPhone().hashCode();
        }

        return "GUEST_" + order.getOrderIdSafe();
    }

    private String extractColorFromModelName(String modelName) {
        if (modelName == null) return "";
        if (modelName.contains("Đen")) return "Đen";
        if (modelName.contains("Trắng")) return "Trắng";
        if (modelName.contains("Xanh")) return "Xanh";
        if (modelName.contains("Đỏ")) return "Đỏ";
        if (modelName.contains("Vàng")) return "Vàng";
        return "";
    }

    private String extractSizeFromModelName(String modelName) {
        if (modelName == null) return "";
        if (modelName.contains("M ") || modelName.contains(",M")) return "M";
        if (modelName.contains("L ") || modelName.contains(",L")) return "L";
        if (modelName.contains("XL")) return "XL";
        if (modelName.contains("XXL")) return "XXL";
        if (modelName.contains("S ") || modelName.contains(",S")) return "S";
        return "";
    }

    private String extractSkuGroup(String sku) {
        if (sku == null || sku.length() < 5) return sku;
        return sku.substring(0, Math.min(5, sku.length()));
    }

    private String getPriceRange(double price) {
        if (price < 100000) return "UNDER_100K";
        if (price < 500000) return "100K_500K";
        if (price < 1000000) return "500K_1M";
        return "OVER_1M";
    }

    // ================================
    // HELPER METHODS - GENERAL
    // ================================

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
        return Boolean.TRUE.equals(value);
    }

    private Long generateKey(String seed) {
        return (long) Math.abs(seed.hashCode());
    }

    private Long getCurrentDateKey() {
        LocalDateTime now = LocalDateTime.now();
        return Long.parseLong(now.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private Long generateDateKey(LocalDateTime dateTime) {
        return Long.parseLong(dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private LocalDateTime timestampToDateTime(Long timestamp) {
        if (timestamp == null) return null;
        try {
            return LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochSecond(timestamp),
                    java.time.ZoneId.of("Asia/Ho_Chi_Minh")
            );
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isPeakHour(LocalDateTime dateTime) {
        int hour = dateTime.getHour();
        return (hour >= 10 && hour <= 14) || (hour >= 18 && hour <= 22);
    }
}