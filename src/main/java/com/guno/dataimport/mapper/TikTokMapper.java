package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.tiktok.*;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.GeographyHelper;
import com.guno.dataimport.util.KeyGenerator;
import com.guno.dataimport.util.OrderStatusValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TikTok Mapper - Maps TikTok API DTOs to internal entities
 * Pattern: Similar to FacebookMapper but with TikTok-specific structure
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TikTokMapper {

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    // ================================
    // CUSTOMER MAPPING
    // ================================

    /**
     * Map TikTok order to Customer entity
     * Note: TikTok doesn't provide full customer details like Facebook,
     * so we extract from recipient_address
     */
    public Customer mapToCustomer(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        TikTokRecipientAddress address = orderDetail.getRecipientAddress();

        // Generate customer ID from user_id or phone
        String customerId = orderDetail.getUserId();
        if (customerId == null || customerId.isEmpty()) {
            // Fallback to phone-based ID
            String phone = address != null ? address.getPhoneNumber() : null;
            customerId = phone != null ? "TIKTOK_" + phone.replaceAll("[^0-9]", "") : "GUEST_" + order.getOrderIdSafe();
        }

        return Customer.builder()
                .customerId(customerId)
                .customerKey((long) customerId.hashCode())
                .platformCustomerId(orderDetail.getUserId())
                .phoneHash(address != null ? address.getPhoneNumber() : "")
                .emailHash(orderDetail.getBuyerEmail())
                .gender("")
                .ageGroup("")
                .customerSegment("TIKTOK")
                .customerTier("STANDARD")
                .acquisitionChannel("TIKTOK")
                .firstOrderDate(convertUnixToLocalDateTime(orderDetail.getCreateTime()))
                .lastOrderDate(convertUnixToLocalDateTime(orderDetail.getUpdateTime()))
                .totalOrders(1)
                .totalSpent(orderDetail.getTotalAmount())
                .averageOrderValue(orderDetail.getTotalAmount())
                .totalItemsPurchased(orderDetail.getLineItemCount())
                .daysSinceFirstOrder(0)
                .daysSinceLastOrder(0)
                .purchaseFrequencyDays(0.0)
                .returnRate(0.0)
                .cancellationRate(0.0)
                .codPreferenceRate(orderDetail.isCashOnDelivery() ? 1.0 : 0.0)
                .favoriteCategory("")
                .favoriteBrand("")
                .preferredPaymentMethod(orderDetail.getPaymentMethodName())
                .preferredPlatform("TIKTOK")
                .primaryShippingProvince(orderDetail.getProvince())
                .shipsToMultipleProvinces(false)
                .loyaltyPoints(0)
                .referralCount(0)
                .isReferrer(false)
                .customerName(address != null ? address.getFullName() : "")
                .build();
    }

    // ================================
    // UTILITY METHODS
    // ================================

    /**
     * Convert Unix timestamp (seconds) to LocalDateTime
     */
    private LocalDateTime convertUnixToLocalDateTime(Long unixTime) {
        if (unixTime == null) return null;
        try {
            return Instant.ofEpochSecond(unixTime)
                    .atZone(ZoneId.of("Asia/Ho_Chi_Minh"))
                    .toLocalDateTime();
        } catch (Exception e) {
            log.warn("Failed to convert unix time: {}", unixTime);
            return null;
        }
    }

    // ================================
    // ORDER MAPPING
    // ================================

    /**
     * Map TikTok order to Order entity
     */
    public Order mapToOrder(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        TikTokPayment payment = orderDetail.getPayment();

        return Order.builder()
                .orderId(order.getOrderIdSafe())
                .customerId(extractCustomerId(order))
                .shopId(order.getShopId() != null ? order.getShopId().toString() : "")
                .internalUuid("TIKTOK")
                .orderCount(1)
                .itemQuantity(calculateTotalQuantity(orderDetail))
                .totalItemsInOrder(orderDetail.getLineItemCount())
                .grossRevenue(orderDetail.getTotalAmount())
                .netRevenue(orderDetail.getTotalAmount())
                .shippingFee(orderDetail.getShippingFee())
                .taxAmount(payment != null ? payment.getTaxAsDouble() : 0.0)
                .discountAmount(orderDetail.getTotalDiscount())
                .codAmount(orderDetail.isCashOnDelivery() ? orderDetail.getTotalAmount() : 0.0)
                .platformFee(0.0)
                .sellerDiscount(payment != null ? payment.getSellerDiscountAsDouble() : 0.0)
                .platformDiscount(payment != null ? payment.getPlatformDiscountAsDouble() : 0.0)
                .originalPrice(payment != null ? payment.getOriginalTotalProductPriceAsDouble() : 0.0)
                .estimatedShippingFee(payment != null ? payment.getOriginalShippingFeeAsDouble() : 0.0)
                .actualShippingFee(orderDetail.getShippingFee())
                .shippingWeightGram(0)
                .daysToShip(0)
                .isDelivered(orderDetail.isDelivered())
                .isCancelled(orderDetail.isCancelled())
                .isReturned(order.hasReturnRefund())
                .isCod(orderDetail.isCashOnDelivery())
                .isNewCustomer(false)
                .isRepeatCustomer(false)
                .isBulkOrder(false)
                .isPromotionalOrder(false)
                .isSameDayDelivery(false)
                .orderToShipHours(0)
                .shipToDeliveryHours(0)
                .totalFulfillmentHours(0)
                .customerOrderSequence(1)
                .customerLifetimeOrders(1)
                .customerLifetimeValue(0.0)
                .daysSinceLastOrder(0)
                .promotionImpact(0.0)
                .adRevenue(0.0)
                .organicRevenue(0.0)
                .aov(orderDetail.getTotalAmount())
                .shippingCostRatio(calculateShippingRatio(orderDetail))
                .createdAt(convertUnixToLocalDateTime(orderDetail.getCreateTime()))
                .orderSource("TIKTOK")
                .platformSpecificData(0)
                .sellerId("")
                .sellerName("")
                .sellerEmail("")
                .latestStatus(order.getStatusSafe() != null ? order.getStatusSafe().longValue() : null)
                .isRefunded(order.hasReturnRefund())
                .refundAmount(extractRefundAmount(order))
                .refundDate(Objects.requireNonNull(extractRefundDate(order)).toLocalDate().toString())
                .isExchanged(false)
                .cancelReason(orderDetail.getCancelReason())
                .build();
    }

    // ================================
    // HELPER METHODS FOR ORDER
    // ================================

    private String extractCustomerId(TikTokOrderDto order) {
        if (!order.hasOrderDetail()) return "";

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        String userId = orderDetail.getUserId();

        if (userId != null && !userId.isEmpty()) {
            return userId;
        }

        // Fallback to phone-based ID
        TikTokRecipientAddress address = orderDetail.getRecipientAddress();
        if (address != null && address.getPhoneNumber() != null) {
            return "TIKTOK_" + address.getPhoneNumber().replaceAll("[^0-9]", "");
        }

        return "GUEST_" + order.getOrderIdSafe();
    }

    private int calculateTotalQuantity(TikTokOrderDetail orderDetail) {
        if (orderDetail == null || !orderDetail.hasLineItems()) return 0;

        return orderDetail.getLineItems().stream()
                .mapToInt(item -> 1) // TikTok doesn't provide quantity per line item
                .sum();
    }

    private Double extractRefundAmount(TikTokOrderDto order) {
        if (!order.hasReturnRefund()) return 0.0;
        return order.getReturnRefund().getTotalRefundAmount();
    }

    private LocalDateTime extractRefundDate(TikTokOrderDto order) {
        if (!order.hasReturnRefund()) return null;
        return convertUnixToLocalDateTime(order.getReturnRefund().getCreateTime());
    }

    private Double calculateShippingRatio(TikTokOrderDetail orderDetail) {
        if (orderDetail == null) return 0.0;

        Double shippingFee = orderDetail.getShippingFee();
        Double totalAmount = orderDetail.getTotalAmount();

        if (totalAmount == null || totalAmount == 0.0) return 0.0;
        return shippingFee / totalAmount;
    }

    // TODO: More mapping methods will be added in next parts

    // ================================
    // ORDER ITEM MAPPING
    // ================================

    /**
     * Map TikTok order to OrderItem entities
     */
    public List<OrderItem> mapToOrderItems(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return new ArrayList<>();
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        if (!orderDetail.hasLineItems()) {
            return new ArrayList<>();
        }

        List<OrderItem> items = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (TikTokLineItem lineItem : orderDetail.getLineItems()) {
            String sku = lineItem.getSku();
            double unitPrice = lineItem.getSalePriceAsDouble();
            int quantity = 1; // TikTok line_items don't have quantity field, assume 1

            items.add(OrderItem.builder()
                    .orderId(order.getOrderIdSafe())
                    .sku(sku)
                    .platformProductId("TT_" + lineItem.getId())
                    .quantity(quantity)
                    .unitPrice(unitPrice)
                    .totalPrice(unitPrice * quantity)
                    .itemDiscount(parseAmount(lineItem.getSellerDiscount()) + parseAmount(lineItem.getPlatformDiscount()))
                    .promotionType("")
                    .promotionCode("")
                    .itemStatus(lineItem.getDisplayStatus())
                    .itemSequence(sequence.getAndIncrement())
                    .opId((long) lineItem.getId().hashCode())
                    .build());
        }
        return items;
    }

    // ================================
    // PRODUCT MAPPING
    // ================================

    /**
     * Map TikTok order to Product entities
     */
    public List<Product> mapToProducts(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return new ArrayList<>();
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        if (!orderDetail.hasLineItems()) {
            return new ArrayList<>();
        }

        List<Product> products = new ArrayList<>();
        for (TikTokLineItem lineItem : orderDetail.getLineItems()) {
            products.add(Product.builder()
                    .sku(!Objects.equals(lineItem.getSellerSku(), "") ? lineItem.getSellerSku() : "UNKNOWN"+lineItem.getId())
                    .platformProductId("TT_" + lineItem.getId())
                    .productId(lineItem.getProductId())
                    .variationId(lineItem.getSellerSku())
                    .barcode("")
                    .productName(lineItem.getProductName())
                    .productDescription("")
                    .brand("")
                    .model("")
                    .categoryLevel1("")
                    .categoryLevel2("")
                    .categoryLevel3("")
                    .categoryPath("")
                    .color(extractColorFromSkuName(lineItem.getSkuName()))
                    .size(extractSizeFromSkuName(lineItem.getSkuName()))
                    .material("")
                    .dimensions("")
                    .weightGram(0)
                    .costPrice(0.0)
                    .retailPrice(lineItem.getSalePriceAsDouble())
                    .originalPrice(lineItem.getOriginalPriceAsDouble())
                    .priceRange(getPriceRange(lineItem.getSalePriceAsDouble()))
                    .isActive(true)
                    .isFeatured(false)
                    .isSeasonal(false)
                    .isNewArrival(false)
                    .isBestSeller(false)
                    .primaryImageUrl(lineItem.getSkuImage())
                    .imageCount(lineItem.getSkuImage() != null ? 1 : 0)
                    .seoTitle("")
                    .seoKeywords("")
                    .skuGroup(!Objects.equals(lineItem.getSellerSku(), "") ? extractSkuGroup(lineItem.getSellerSku()) : "")
                    .build());
        }
        return products;
    }

    // ================================
    // HELPER METHODS FOR ITEMS/PRODUCTS
    // ================================

    private Double parseAmount(String amount) {
        if (amount == null || amount.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(amount);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private String getPriceRange(Double price) {
        if (price == null || price == 0.0) return "0";
        if (price < 100000) return "0-100K";
        if (price < 300000) return "100K-300K";
        if (price < 500000) return "300K-500K";
        if (price < 1000000) return "500K-1M";
        return "1M+";
    }

    /**
     * Extract color from sku_name (e.g. "Đỏ, Nữ M (52-59KG)")
     * TikTok format: "Color, Size" or just "Color"
     */
    private String extractColorFromSkuName(String skuName) {
        if (skuName == null || skuName.isEmpty()) return "";

        // Split by comma to get first part (usually color)
        String[] parts = skuName.split(",");
        if (parts.length > 0) {
            return parts[0].trim();
        }
        return "";
    }

    /**
     * Extract size from sku_name (e.g. "Đỏ, Nữ M (52-59KG)")
     * TikTok format: second part after comma
     */
    private String extractSizeFromSkuName(String skuName) {
        if (skuName == null || skuName.isEmpty()) return "";

        // Split by comma to get second part (usually size)
        String[] parts = skuName.split(",");
        if (parts.length > 1) {
            return parts[1].trim();
        }
        return "";
    }

    private String extractSkuGroup(String sellerSku) {
        if (sellerSku == null || sellerSku.isEmpty()) {
            return "";
        }

        sellerSku = sellerSku.trim();

        // Pattern: ^(\d{2,3}[A-Z]{2,3}\d{2,3})
        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile("^(\\d{2,3}[A-Z]{2,3}\\d{2,3})");
        java.util.regex.Matcher matcher = pattern.matcher(sellerSku);

        if (matcher.find()) {
            return matcher.group(1);
        }

        // Fallback: first 7 chars if length >= 7
        if (sellerSku.length() >= 7) {
            return sellerSku.substring(0, 7);
        }

        return sellerSku;
    }

    // ================================
    // GEOGRAPHY MAPPING
    // ================================

    /**
     * Map TikTok order to GeographyInfo entity
     */
    public GeographyInfo mapToGeographyInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        String province = orderDetail.getProvince();
        String district = orderDetail.getDistrict();

        return GeographyInfo.builder()
                .orderId(order.getOrderIdSafe())
                .geographyKey(KeyGenerator.generateGeographyKey(province, district))
                .countryCode("VN")
                .countryName("Vietnam")
                .provinceName(province)
                .districtName(district)
                .wardName(orderDetail.getWard())
                .isUrban(GeographyHelper.isUrbanProvince(province))
                .isMetropolitan(GeographyHelper.isMetroProvince(province))
                .economicTier(GeographyHelper.getEconomicTier(province))
                .shippingZone(GeographyHelper.getShippingZone(province))
                .standardDeliveryDays(GeographyHelper.getDeliveryDays(province))
                .expressDeliveryAvailable(true)
                .latitude(0.0)
                .longitude(0.0)
                .regionCode("")
                .regionName("")
                .provinceCode("")
                .provinceType("")
                .districtCode("")
                .districtType("")
                .wardCode("")
                .wardType("")
                .isCoastal(false)
                .isBorder(false)
                .populationDensity("")
                .incomeLevel("")
                .deliveryComplexity("")
                .build();
    }

    // ================================
    // PAYMENT MAPPING
    // ================================

    /**
     * Map TikTok order to PaymentInfo entity
     */
    public PaymentInfo mapToPaymentInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        boolean isCod = orderDetail.isCashOnDelivery();
        String method = isCod ? "COD" : "ONLINE";
        String provider = isCod ? "CASH" : "TIKTOK_PAY";
        String category = isCod ? "CASH_ON_DELIVERY" : "ONLINE_PAYMENT";

        return PaymentInfo.builder()
                .orderId(order.getOrderIdSafe())
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
    // SHIPPING MAPPING
    // ================================

    /**
     * Map TikTok order to ShippingInfo entity
     */
    public ShippingInfo mapToShippingInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        String providerId = orderDetail.getShippingProviderId();
        String providerName = orderDetail.getShippingProvider();
        String serviceType = "STANDARD";

        if (providerId == null || providerId.isEmpty()) {
            providerId = "TIKTOK_LOGISTICS";
            providerName = "TikTok Logistics";
        }

        return ShippingInfo.builder()
                .orderId(order.getOrderIdSafe())
                .shippingKey(KeyGenerator.generateShippingKey(providerId, serviceType))
                .providerId(providerId)
                .providerName(providerName)
                .providerType("MARKETPLACE")
                .providerTier("STANDARD")
                .serviceType(serviceType)
                .serviceTier("STANDARD")
                .deliveryCommitment("")
                .shippingMethod("STANDARD")
                .pickupType("")
                .deliveryType(orderDetail.getDeliveryType())
                .baseFee(orderDetail.getShippingFee())
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsCod(orderDetail.isCashOnDelivery())
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(true)
                .providesSmsUpdates(false)
                .averageDeliveryDays(0.0)
                .onTimeDeliveryRate(0.0)
                .successDeliveryRate(0.0)
                .damageRate(0.0)
                .coverageProvinces(orderDetail.getProvince())
                .coverageNationwide(false)
                .coverageInternational(false)
                .build();
    }

    // ================================
    // PROCESSING DATE MAPPING
    // ================================

    /**
     * Map TikTok order to ProcessingDateInfo entity
     */
    public ProcessingDateInfo mapToProcessingDateInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail()) {
            return null;
        }

        TikTokOrderDetail orderDetail = order.getOrderDetail();
        LocalDateTime orderDate = convertUnixToLocalDateTime(orderDetail.getCreateTime());

        if (orderDate == null) {
            orderDate = LocalDateTime.now();
        }

        return ProcessingDateInfo.builder()
                .orderId(order.getOrderIdSafe())
                .dateKey(generateDateKey(orderDate))
                .fullDate(orderDate.toLocalDate().toString())
                .dayOfWeek(orderDate.getDayOfWeek().getValue())
                .dayOfWeekName(orderDate.getDayOfWeek().name())
                .dayOfMonth(orderDate.getDayOfMonth())
                .dayOfYear(orderDate.getDayOfYear())
                .weekOfYear(orderDate.get(WeekFields.ISO.weekOfWeekBasedYear()))
                .monthOfYear(orderDate.getMonthValue())
                .monthName(orderDate.getMonth().name())
                .quarterOfYear((orderDate.getMonthValue() - 1) / 3 + 1)
                .quarterName("Q" + ((orderDate.getMonthValue() - 1) / 3 + 1))
                .year(orderDate.getYear())
                .isWeekend(orderDate.getDayOfWeek().getValue() >= 6)
                .isHoliday(false)
                .isBusinessDay(orderDate.getDayOfWeek().getValue() < 6)
                .fiscalYear(orderDate.getYear())
                .fiscalQuarter((orderDate.getMonthValue() - 1) / 3 + 1)
                .seasonName(getSeason(orderDate.getMonthValue()))
                .build();
    }

    // ================================
    // ORDER STATUS MAPPING
    // ================================

    /**
     * Map TikTok order to OrderStatus entities
     */
    public List<OrderStatus> mapToOrderStatus(TikTokOrderDto order) {
        if (order == null) {
            return new ArrayList<>();
        }

        List<OrderStatus> orderStatuses = new ArrayList<>();
        Integer currentStatus = order.getStatusSafe();

        if (currentStatus != null) {
            String subStatusId = "0"; // TikTok doesn't have sub_status
            Integer partnerStatusId = 0; // Would need to parse from tracking if available

            OrderStatus orderStatus = OrderStatus.builder()
                    .statusKey((long) currentStatus)
                    .orderId(order.getOrderIdSafe())
                    .subStatusId(subStatusId)
                    .partnerStatusId(partnerStatusId)
                    .transitionDateKey(getCurrentDateKey())
                    .transitionTimestamp(LocalDateTime.now())
                    .durationInPreviousStatusHours(0)
                    .transitionReason("ORDER_CREATED")
                    .transitionTrigger("SYSTEM")
                    .changedBy("TIKTOK_API")
                    .isOnTimeTransition(true)
                    .isExpectedTransition(true)
                    .historyKey((long) ("HIST_" + order.getOrderIdSafe()).hashCode())
                    .createdAt(order.getInsertedAt() != null ? order.getInsertedAt() : "")
                    .build();
            orderStatuses.add(orderStatus);
        }

        return orderStatuses;
    }

    // ================================
    // FINAL UTILITY METHODS
    // ================================

    private Long generateDateKey(LocalDateTime dateTime) {
        return Long.parseLong(dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private Integer getCurrentDateKey() {
        return Integer.parseInt(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private String getSeason(int month) {
        if (month >= 3 && month <= 5) return "SPRING";
        if (month >= 6 && month <= 8) return "SUMMER";
        if (month >= 9 && month <= 11) return "FALL";
        return "WINTER";
    }
}