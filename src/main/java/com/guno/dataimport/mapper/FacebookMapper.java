package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.facebook.*;
import com.guno.dataimport.entity.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facebook Mapper - Convert Facebook DTOs to Database Entities
 * IMPROVED: Better null handling, more accurate mapping, simplified logic
 */
@Component
@Slf4j
public class FacebookMapper {

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    // IMPROVED: Better null handling and simplified logic
    public Customer mapToCustomer(FacebookOrderDto order) {
        if (order == null || order.getCustomer() == null) return null;

        FacebookCustomer customer = order.getCustomer();
        return Customer.builder()
                .customerId(customer.getId())
                .customerKey(generateKey(customer.getId()))
                .platformCustomerId(customer.getId())
                .phoneHash(hashValue(customer.getPrimaryPhone()))
                .emailHash(hashValue(customer.getPrimaryEmail()))
                .gender(normalizeGender(customer.getGender()))
                .customerSegment("FACEBOOK")
                .customerTier("STANDARD")
                .acquisitionChannel("FACEBOOK")
                .firstOrderDate(parseDateTime(customer.getInsertedAt()))
                .lastOrderDate(parseDateTime(customer.getLastOrderAt()))
                .totalOrders(safeInt(customer.getOrderCount()))
                .totalSpent(customer.getPurchasedAmountAsDouble())
                .averageOrderValue(calculateAOV(customer))
                .daysSinceFirstOrder(calculateDaysSince(customer.getInsertedAt()))
                .daysSinceLastOrder(calculateDaysSince(customer.getLastOrderAt()))
                .returnRate(calculateReturnRate(customer))
                .preferredPlatform("FACEBOOK")
                .primaryShippingProvince(getProvinceName(order))
                .loyaltyPoints(safeInt(customer.getRewardPoint()))
                .referralCount(safeInt(customer.getCountReferrals()))
                .isReferrer(safeBool(customer.getIsReferrer()))
                .build();
    }

    // IMPROVED: More accurate financial calculations
    public Order mapToOrder(FacebookOrderDto order) {
        if (order == null) return null;

        double totalAmount = order.getTotalAmountAsDouble();
        double discount = safeDouble(order.getDiscount());
        double grossRevenue = totalAmount + discount; // Before discount

        return Order.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomer() != null ? order.getCustomer().getId() : null)
                .shopId("FACEBOOK_SHOP")
                .internalUuid("FB_" + order.getOrderId())
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
                .shippingCostRatio(calculateRatio(safeDouble(order.getShippingFee()), totalAmount))
                .createdAt(parseDateTime(order.getCreatedAt()))
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
                    .platformProductId("FB_" + item.getId())
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
                    .platformProductId("FB_" + item.getId())
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
                .paymentProvider("FACEBOOK_PAY")
                .isCod(isCod)
                .isPrepaid(!isCod)
                .supportsRefund(true)
                .supportsPartialRefund(true)
                .refundProcessingDays(7)
                .riskLevel("LOW")
                .settlementDays(1)
                .build();
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
}