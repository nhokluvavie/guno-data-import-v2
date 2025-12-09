package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.tiktok.*;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.GeographyHelper;
import com.guno.dataimport.util.KeyGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TikTok Mapper - Maps TikTok API DTOs to internal entities
 * VERIFIED: All field names and types match TikTokOrderDetail and TikTokLineItem
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TikTokMapper {

    // Status codes - Return/Refund (PRIORITY 1)
    private static final Long STATUS_REFUNDED = 18L;
    private static final Long STATUS_RETURN_AND_REFUND = 19L;
    private static final Long STATUS_REPLACEMENT = 20L;

    // Status codes - Order (PRIORITY 2)
    private static final Long STATUS_NEW = 0L;
    private static final Long STATUS_CONFIRMED = 1L;
    private static final Long STATUS_SHIPPED = 2L;
    private static final Long STATUS_DELIVERED = 3L;
    private static final Long STATUS_CANCELED = 6L;
    private static final Long STATUS_WAITING_PICKUP = 9L;
    private static final Long STATUS_COLLECTED_MONEY = 16L;
    private static final Long STATUS_WAITING_CONFIRMATION = 17L;

    // ================================
    // STATUS MAPPING
    // ================================

    private Long mapTikTokToLatestStatus(TikTokOrderDto order) {
        if (order == null || !order.hasTikTokData()) return STATUS_NEW;

        // PRIORITY 1: Return/Refund
        if (order.hasReturnRefund()) {
            String returnType = order.getReturnRefund().getReturnType();
            if (returnType != null) {
                Long returnStatus = switch (returnType.toUpperCase()) {
                    case "REFUND" -> STATUS_REFUNDED;
                    case "RETURN_AND_REFUND" -> STATUS_RETURN_AND_REFUND;
                    case "REPLACEMENT" -> STATUS_REPLACEMENT;
                    default -> null;
                };
                if (returnStatus != null) return returnStatus;
            }
        }

        // PRIORITY 2: Order Status (field: "status")
        TikTokOrderDetail detail = order.getOrderDetail();
        if (detail != null && detail.getStatus() != null) {
            return switch (detail.getStatus().toUpperCase()) {
                case "UNPAID" -> STATUS_NEW;
                case "ON_HOLD" -> STATUS_WAITING_CONFIRMATION;
                case "AWAITING_SHIPMENT" -> STATUS_CONFIRMED;
                case "AWAITING_COLLECTION" -> STATUS_WAITING_PICKUP;
                case "IN_TRANSIT" -> STATUS_SHIPPED;
                case "DELIVERED" -> STATUS_DELIVERED;
                case "COMPLETED" -> STATUS_COLLECTED_MONEY;
                case "CANCEL" -> STATUS_CANCELED;
                default -> STATUS_NEW;
            };
        }

        return STATUS_NEW;
    }

    // ================================
    // CUSTOMER MAPPING
    // ================================

    public Customer mapToCustomer(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        TikTokRecipientAddress address = detail.getRecipientAddress();
        String customerId = extractCustomerId(order);
        Double totalAmount = detail.getTotalAmount();

        return Customer.builder()
                .customerId(customerId)
                .customerKey((long) customerId.hashCode())
                .platformCustomerId(detail.getUserId())
                .phoneHash(address != null ? address.getPhoneNumber() : "")
                .emailHash(detail.getBuyerEmail())
                .gender("")
                .ageGroup("")
                .customerSegment("TIKTOK")
                .customerTier("STANDARD")
                .acquisitionChannel("TIKTOK")
                .firstOrderDate(formatTimestampSafe(detail.getCreateTime()))
                .lastOrderDate(formatTimestampSafe(detail.getUpdateTime()))
                .totalOrders(1)
                .totalSpent(totalAmount)
                .averageOrderValue(totalAmount)
                .totalItemsPurchased(detail.getLineItemCount())
                .daysSinceFirstOrder(0)
                .daysSinceLastOrder(0)
                .purchaseFrequencyDays(0.0)
                .returnRate(0.0)
                .cancellationRate(0.0)
                .codPreferenceRate(detail.isCashOnDelivery() ? 1.0 : 0.0)
                .favoriteCategory("")
                .favoriteBrand("")
                .preferredPaymentMethod(detail.getPaymentMethodName())
                .preferredPlatform("TIKTOK")
                .primaryShippingProvince(detail.getProvince())
                .shipsToMultipleProvinces(false)
                .loyaltyPoints(0)
                .referralCount(0)
                .isReferrer(false)
                .customerName(address != null ? address.getFullName() : "")
                .build();
    }

    // ================================
    // ORDER MAPPING
    // ================================

    public Order mapToOrder(TikTokOrderDto order) {
        if (order == null || !order.hasTikTokData()) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        if (detail == null || detail.getOrderId() == null) {
            log.warn("Order {} has no orderDetail", order.getOrderId());
            return null;
        }

        TikTokReturnRefund refund = order.getReturnRefund();
        Double totalAmount = detail.getTotalAmount();
        Double shippingFee = detail.getShippingFee();

        return Order.builder()
                .orderId(order.getOrderIdSafe())
                .customerId(extractCustomerId(order))
                .shopId("TIKTOK_SHOP")
                .internalUuid("TIKTOK")
                .orderCount(1)
                .itemQuantity(getTotalQuantity(detail))
                .totalItemsInOrder(getTotalQuantity(detail))
                .grossRevenue(totalAmount)
                .netRevenue(totalAmount)
                .shippingFee(shippingFee)
                .taxAmount(0.0)
                .discountAmount(detail.getTotalDiscount())
                .codAmount(detail.isCashOnDelivery() ? totalAmount : 0.0)
                .platformFee(0.0)
                .sellerDiscount(0.0)
                .platformDiscount(0.0)
                .originalPrice(totalAmount)
                .estimatedShippingFee(0.0)
                .actualShippingFee(shippingFee)
                .shippingWeightGram(0)
                .daysToShip(0)
                .isDelivered("DELIVERED".equals(detail.getStatus()) || "COMPLETED".equals(detail.getStatus()))
                .isCancelled("CANCEL".equals(detail.getStatus()))
                .isReturned(refund != null && "RETURN_AND_REFUND".equals(refund.getReturnType()))
                .isCod(detail.isCashOnDelivery())
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
                .orderSource("TIKTOK")
                .platformSpecificData(order.getStatus())
                .sellerId("TIKTOK")
                .sellerName("TikTok Shop")
                .sellerEmail("tiktok@shop.com")
                .latestStatus(mapTikTokToLatestStatus(order))
                .isRefunded(refund != null && refund.getReturnType() != null && refund.getReturnType().contains("REFUND"))
                .refundAmount(extractRefundAmount(refund))
                .refundDate(extractRefundDate(refund))
                .isExchanged(refund != null && "REPLACEMENT".equals(refund.getReturnType()))
                .cancelReason("CANCEL".equals(detail.getStatus()) ? "USER_CANCELLED" : null)
                .build();
    }

    // ================================
    // ORDER ITEMS
    // ================================

    public List<OrderItem> mapToOrderItems(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return new ArrayList<>();

        TikTokOrderDetail detail = order.getOrderDetail();
        if (!detail.hasLineItems()) return new ArrayList<>();

        List<OrderItem> items = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (TikTokLineItem lineItem : detail.getLineItems()) {
            String sku = !Objects.equals(lineItem.getSellerSku(), "") ?
                    lineItem.getSellerSku() : "UNKNOWN" + lineItem.getId();
            double unitPrice = lineItem.getSalePriceAsDouble();
            int quantity = 1;

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
    // PRODUCTS
    // ================================

    public List<Product> mapToProducts(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return new ArrayList<>();

        TikTokOrderDetail detail = order.getOrderDetail();
        if (!detail.hasLineItems()) return new ArrayList<>();

        List<Product> products = new ArrayList<>();
        for (TikTokLineItem lineItem : detail.getLineItems()) {
            products.add(Product.builder()
                    .sku(!Objects.equals(lineItem.getSellerSku(), "") ?
                            lineItem.getSellerSku() : "UNKNOWN" + lineItem.getId())
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
                    .weightGram(0)
                    .dimensions("")
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
                    .skuGroup(!Objects.equals(lineItem.getSellerSku(), "") ?
                            extractSkuGroup(lineItem.getSellerSku()) : "")
                    .build());
        }
        return products;
    }

    // ================================
    // GEOGRAPHY
    // ================================

    public GeographyInfo mapToGeographyInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        String province = detail.getProvince();
        String district = detail.getDistrict();

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
                .wardName(detail.getWard())
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
    // PAYMENT
    // ================================

    public PaymentInfo mapToPaymentInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        boolean isCod = detail.isCashOnDelivery();
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
    // SHIPPING
    // ================================

    public ShippingInfo mapToShippingInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        String providerId = detail.getShippingProviderId();
        String providerName = detail.getShippingProvider();

        if (providerId == null || providerId.isEmpty()) {
            providerId = "TIKTOK_LOGISTICS";
            providerName = "TikTok Logistics";
        }

        return ShippingInfo.builder()
                .orderId(order.getOrderIdSafe())
                .shippingKey(KeyGenerator.generateShippingKey(providerId, "STANDARD"))
                .providerId(providerId)
                .providerName(providerName)
                .providerType("THIRD_PARTY")
                .providerTier("STANDARD")
                .serviceType("STANDARD")
                .serviceTier("REGULAR")
                .deliveryCommitment("")
                .shippingMethod("STANDARD")
                .pickupType("PICKUP")
                .deliveryType("HOME")
                .baseFee(detail.getShippingFee())
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsCod(detail.isCashOnDelivery())
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(true)
                .providesSmsUpdates(true)
                .averageDeliveryDays(3.0)
                .onTimeDeliveryRate(0.95)
                .successDeliveryRate(0.98)
                .damageRate(0.01)
                .coverageProvinces("")
                .coverageNationwide(true)
                .coverageInternational(false)
                .build();
    }

    // ================================
    // PROCESSING DATE
    // ================================

    public ProcessingDateInfo mapToProcessingDateInfo(TikTokOrderDto order) {
        if (order == null || !order.hasOrderDetail() || order.getOrderDetail().getOrderId() == null) return null;

        TikTokOrderDetail detail = order.getOrderDetail();
        LocalDateTime orderDate = formatTimestampSafe(detail.getCreateTime());
        if (orderDate == null) orderDate = LocalDateTime.now();

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
                .holidayName("")
                .isBusinessDay(orderDate.getDayOfWeek().getValue() < 6)
                .fiscalYear(orderDate.getYear())
                .fiscalQuarter((orderDate.getMonthValue() - 1) / 3 + 1)
                .isShoppingSeason(false)
                .seasonName(getSeason(orderDate.getMonthValue()))
                .isPeakHour(false)
                .hourOfDay(orderDate.getHour())
                .build();
    }

    // ================================
    // ORDER STATUS
    // ================================

    public List<OrderStatus> mapToOrderStatus(TikTokOrderDto order) {
        if (order == null || order.getOrderDetail().getOrderId() == null) return new ArrayList<>();

        Long statusKey = mapTikTokToLatestStatus(order);
        if (statusKey == null) return new ArrayList<>();

        return Collections.singletonList(OrderStatus.builder()
                .statusKey(statusKey)
                .orderId(order.getOrderIdSafe())
                .subStatusId("0")
                .partnerStatusId(0)
                .transitionDateKey(generateDateKey(LocalDateTime.now()))
                .transitionTimestamp(LocalDateTime.now())
                .durationInPreviousStatusHours(0)
                .transitionReason("ORDER_CREATED")
                .transitionTrigger("SYSTEM")
                .changedBy("TIKTOK_API")
                .isOnTimeTransition(true)
                .isExpectedTransition(true)
                .historyKey((long) ("HIST_" + order.getOrderIdSafe()).hashCode())
                .createdAt(order.getInsertedAt() != null ? String.valueOf(order.getInsertedAt()) : "")
                .build());
    }

    // ================================
    // HELPERS
    // ================================

    private String extractCustomerId(TikTokOrderDto order) {
        TikTokOrderDetail detail = order.getOrderDetail();
        if (detail == null) return "GUEST_" + order.getOrderIdSafe();

        String userId = detail.getUserId();
        if (userId != null && !userId.isEmpty()) return "TIKTOK_" + userId;

        TikTokRecipientAddress address = detail.getRecipientAddress();
        if (address != null && address.getPhoneNumber() != null) {
            String phone = address.getPhoneNumber().replaceAll("[^0-9]", "");
            if (!phone.isEmpty()) return "TIKTOK_" + phone;
        }

        return "GUEST_" + order.getOrderIdSafe();
    }

    private int getTotalQuantity(TikTokOrderDetail detail) {
        if (detail == null || !detail.hasLineItems()) return 1;
        return detail.getLineItems().size();
    }

    private String getPriceRange(Double price) {
        if (price == null || price == 0.0) return "0";
        if (price < 100000) return "0-100K";
        if (price < 300000) return "100K-300K";
        if (price < 500000) return "300K-500K";
        if (price < 1000000) return "500K-1M";
        return "1M+";
    }

    private String extractColorFromSkuName(String skuName) {
        if (skuName == null || skuName.isEmpty()) return "";
        String[] parts = skuName.split(",");
        if (parts.length > 0) return parts[0].trim();
        return "";
    }

    private String extractSizeFromSkuName(String skuName) {
        if (skuName == null || skuName.isEmpty()) return "";
        String[] parts = skuName.split(",");
        if (parts.length > 1) return parts[1].trim();
        return "";
    }

    private String extractSkuGroup(String sellerSku) {
        if (sellerSku == null || sellerSku.isEmpty()) return "";
        sellerSku = sellerSku.trim();

        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile("^(\\d{2,3}[A-Z]{2,3}\\d{2,3})");
        java.util.regex.Matcher matcher = pattern.matcher(sellerSku);

        if (matcher.find()) return matcher.group(1);
        if (sellerSku.length() >= 7) return sellerSku.substring(0, 7);
        return sellerSku;
    }

    private Double parseAmount(String amount) {
        if (amount == null || amount.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(amount);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private LocalDateTime formatTimestampSafe(Long timestamp) {
        if (timestamp == null) return null;
        try {
            return LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.of("+07:00"));
        } catch (Exception e) {
            log.warn("Failed to parse timestamp: {}", timestamp);
            return null;
        }
    }

    private Double extractRefundAmount(TikTokReturnRefund refund) {
        if (refund == null || refund.getRefundAmount() == null) return null;
        try {
            String refundTotal = refund.getRefundAmount().getRefundTotal();
            return refundTotal != null ? Double.parseDouble(refundTotal) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String extractRefundDate(TikTokReturnRefund refund) {
        if (refund == null || refund.getUpdateTime() == null) return null;
        try {
            LocalDateTime refundDateTime = formatTimestampSafe(refund.getUpdateTime());
            return refundDateTime != null ? refundDateTime.toString() : null;
        } catch (Exception e) {
            log.warn("Failed to extract refund date: {}", refund.getUpdateTime());
            return null;
        }
    }

    private Long generateDateKey(LocalDateTime dateTime) {
        return Long.parseLong(dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }

    private String getSeason(int month) {
        if (month >= 3 && month <= 5) return "SPRING";
        if (month >= 6 && month <= 8) return "SUMMER";
        if (month >= 9 && month <= 11) return "FALL";
        return "WINTER";
    }
}