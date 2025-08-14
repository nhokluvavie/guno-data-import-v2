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
 * Based on actual facebook_order.json structure
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

    // Map Facebook order to Customer entity
    public Customer mapToCustomer(FacebookOrderDto order) {
        if (order == null || order.getCustomer() == null) {
            return null;
        }

        FacebookCustomer fbCustomer = order.getCustomer();

        return Customer.builder()
                .customerId(fbCustomer.getId())
                .customerKey(generateCustomerKey(fbCustomer.getId()))
                .platformCustomerId(fbCustomer.getId())
                .phoneHash(hashPhone(fbCustomer.getPrimaryPhone()))
                .emailHash(hashEmail(fbCustomer.getPrimaryEmail()))
                .gender(normalizeGender(fbCustomer.getGender()))
                .customerSegment("FACEBOOK")
                .customerTier("STANDARD")
                .acquisitionChannel("FACEBOOK")
                .firstOrderDate(parseDateTime(fbCustomer.getInsertedAt()))
                .lastOrderDate(parseDateTime(fbCustomer.getLastOrderAt()))
                .totalOrders(fbCustomer.getOrderCount() != null ? fbCustomer.getOrderCount() : 0)
                .totalSpent(fbCustomer.getPurchasedAmountAsDouble())
                .averageOrderValue(calculateAOV(fbCustomer))
                .totalItemsPurchased(0) // Will be calculated later
                .daysSinceFirstOrder(calculateDaysSinceFirst(fbCustomer.getInsertedAt()))
                .daysSinceLastOrder(calculateDaysSinceLast(fbCustomer.getLastOrderAt()))
                .purchaseFrequencyDays(0.0) // Will be calculated
                .returnRate(calculateReturnRate(fbCustomer))
                .cancellationRate(0.0) // Default
                .codPreferenceRate(0.0) // Will be calculated
                .favoriteCategory("") // Not available in Facebook
                .favoriteBrand("") // Not available
                .preferredPaymentMethod("") // Will be determined from orders
                .preferredPlatform("FACEBOOK")
                .primaryShippingProvince(extractPrimaryProvince(order))
                .shipsToMultipleProvinces(false) // Default
                .loyaltyPoints(fbCustomer.getRewardPoint() != null ? fbCustomer.getRewardPoint() : 0)
                .referralCount(fbCustomer.getCountReferrals() != null ? fbCustomer.getCountReferrals() : 0)
                .isReferrer(fbCustomer.getIsReferrer() != null ? fbCustomer.getIsReferrer() : false)
                .build();
    }

    // Map Facebook order to Order entity - CORRECTED MAPPING
    public Order mapToOrder(FacebookOrderDto order) {
        if (order == null) {
            return null;
        }

        return Order.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomer() != null ? order.getCustomer().getId() : null)
                .shopId("FACEBOOK_SHOP")
                .internalUuid(generateInternalUuid(order))
                .orderCount(1)
                .itemQuantity(calculateItemQuantity(order.getItems()))
                .totalItemsInOrder(order.getItems() != null ? order.getItems().size() : 0)

                // CORRECTED: Gross Revenue = Total BEFORE discount
                .grossRevenue(calculateGrossRevenue(order))
                // CORRECTED: Net Revenue = Total AFTER discount
                .netRevenue(order.getTotalAmountAsDouble()) // total_price_after_sub_discount

                .shippingFee(order.getShippingFee() != null ? order.getShippingFee().doubleValue() : 0.0)
                .taxAmount(order.getTax() != null ? order.getTax().doubleValue() : 0.0)
                .discountAmount(order.getDiscount() != null ? order.getDiscount().doubleValue() : 0.0)

                // CORRECTED: COD amount only if COD order
                .codAmount(order.isCodOrder() ? order.getTotalAmountAsDouble() : 0.0)

                .platformFee(0.0) // Not provided by Facebook
                .sellerDiscount(0.0) // Default
                .platformDiscount(order.getDiscount() != null ? order.getDiscount().doubleValue() : 0.0)

                // CORRECTED: Original price = gross revenue (before discount)
                .originalPrice(calculateGrossRevenue(order))

                .estimatedShippingFee(order.getShippingFee() != null ? order.getShippingFee().doubleValue() : 0.0)
                .actualShippingFee(order.getShippingFee() != null ? order.getShippingFee().doubleValue() : 0.0)
                .shippingWeightGram(calculateTotalWeight(order.getItems()))
                .daysToShip(1) // Default for Facebook
                .isDelivered(isOrderDelivered(order.getStatus()))
                .isCancelled(isOrderCancelled(order.getStatus()))
                .isReturned(false) // Default
                .isCod(order.isCodOrder())
                .isNewCustomer(isNewCustomer(order.getCustomer()))
                .isRepeatCustomer(!isNewCustomer(order.getCustomer()))
                .isBulkOrder(false) // Default
                .isPromotionalOrder(order.getDiscount() != null && order.getDiscount() > 0)
                .isSameDayDelivery(false) // Default
                .orderToShipHours(24) // Default
                .shipToDeliveryHours(72) // Default
                .totalFulfillmentHours(96) // Default
                .customerOrderSequence(1) // Will be calculated
                .customerLifetimeOrders(order.getCustomer() != null ?
                        (order.getCustomer().getOrderCount() != null ? order.getCustomer().getOrderCount() : 1) : 1)
                .customerLifetimeValue(order.getCustomer() != null ?
                        order.getCustomer().getPurchasedAmountAsDouble() : 0.0)
                .daysSinceLastOrder(0) // Will be calculated
                .promotionImpact(order.getDiscount() != null ? order.getDiscount().doubleValue() : 0.0)

                // CORRECTED: Ad revenue tracking
                .adRevenue(order.getAdId() != null && !order.getAdId().isEmpty() ?
                        order.getTotalAmountAsDouble() : 0.0)
                .organicRevenue(order.getAdId() == null || order.getAdId().isEmpty() ?
                        order.getTotalAmountAsDouble() : 0.0)

                .aov(order.getTotalAmountAsDouble())
                .shippingCostRatio(calculateShippingCostRatio(order))
                .createdAt(parseDateTime(order.getCreatedAt()))
                .rawData(0) // Default
                .platformSpecificData(0) // Default
                .build();
    }

    // Map Facebook items to OrderItem entities - CORRECTED
    public List<OrderItem> mapToOrderItems(FacebookOrderDto order) {
        if (order == null || order.getItems() == null || order.getItems().isEmpty()) {
            return new ArrayList<>();
        }

        List<OrderItem> orderItems = new ArrayList<>();
        AtomicInteger sequence = new AtomicInteger(1);

        for (FacebookItemDto item : order.getItems()) {
            OrderItem orderItem = OrderItem.builder()
                    .orderId(order.getOrderId())
                    .sku(item.getProductKey())
                    .platformProductId("FACEBOOK_" + item.getId())
                    .quantity(item.getQuantityOrDefault())
                    .unitPrice(item.getPriceAsDouble())

                    // CORRECTED: Calculate total price correctly
                    .totalPrice(item.getPriceAsDouble() * item.getQuantityOrDefault())

                    .itemDiscount(item.getDiscountAmount() != null ? item.getDiscountAmount().doubleValue() : 0.0)
                    .promotionType("") // Not available in Facebook
                    .promotionCode("") // Not available
                    .itemStatus(item.getStatus() != null ? item.getStatus() : "ACTIVE")
                    .itemSequence(sequence.getAndIncrement())
                    .opId(0L) // Default
                    .build();

            orderItems.add(orderItem);
        }

        return orderItems;
    }

    // Map Facebook items to Product entities - CORRECTED
    public List<Product> mapToProducts(FacebookOrderDto order) {
        if (order == null || order.getItems() == null || order.getItems().isEmpty()) {
            return new ArrayList<>();
        }

        List<Product> products = new ArrayList<>();

        for (FacebookItemDto item : order.getItems()) {
            Product product = Product.builder()
                    .sku(item.getProductKey())
                    .platformProductId("FACEBOOK_" + item.getId())
                    .productId(item.getProductId())
                    .variationId(item.getVariantId())
                    .barcode(item.getBarcode())
                    .productName(item.getName() != null ? item.getName() : item.getProductName())
                    .productDescription("") // Not available
                    .brand(item.getBrand())
                    .model(item.getModel())
                    .categoryLevel1(item.getCategory())
                    .categoryLevel2("") // Not available
                    .categoryLevel3("") // Not available
                    .categoryPath(item.getCategory())
                    .color(item.getColor())
                    .size(item.getSize())
                    .material(item.getMaterial())
                    .weightGram(item.getWeightGram() != null ? item.getWeightGram() : 0)
                    .dimensions(item.getDimensions())

                    // CORRECTED: Price mapping
                    .costPrice(item.getCostPrice() != null ? item.getCostPrice().doubleValue() : 0.0)
                    .retailPrice(item.getRetailPrice() != null ? item.getRetailPrice().doubleValue() :
                            item.getPriceAsDouble())
                    .originalPrice(item.getOriginalPrice() != null ? item.getOriginalPrice().doubleValue() :
                            (item.getPriceAsDouble() + (item.getDiscountAmount() != null ? item.getDiscountAmount().doubleValue() : 0.0)))

                    .priceRange(calculatePriceRange(item))
                    .isActive(item.getIsActive() != null ? item.getIsActive() : true)
                    .isFeatured(item.getIsFeatured() != null ? item.getIsFeatured() : false)
                    .isSeasonal(false) // Default
                    .isNewArrival(item.getIsNewArrival() != null ? item.getIsNewArrival() : false)
                    .isBestSeller(item.getIsBestSeller() != null ? item.getIsBestSeller() : false)
                    .primaryImageUrl(item.getImageUrl() != null ? item.getImageUrl() : item.getPrimaryImageUrl())
                    .imageCount(item.getImageCount() != null ? item.getImageCount() : 0)
                    .seoTitle("") // Not available
                    .seoKeywords("") // Not available
                    .build();

            products.add(product);
        }

        return products;
    }

    // Map Facebook order to GeographyInfo entity - CORRECTED
    public GeographyInfo mapToGeographyInfo(FacebookOrderDto order) {
        if (order == null) {
            return null;
        }

        return GeographyInfo.builder()
                .orderId(order.getOrderId())
                .geographyKey(generateGeographyKey(order.getOrderId()))
                .countryCode("VN") // Default Vietnam
                .countryName("Vietnam")
                .regionCode("") // Not available
                .regionName("") // Not available
                .provinceCode(order.getNewProvinceId())
                .provinceName(order.getNewProvinceName())
                .provinceType("") // Not available
                .districtCode("") // Not available
                .districtName(order.getNewDistrictName())
                .districtType("") // Not available
                .wardCode(order.getNewCommuneId())
                .wardName("") // Not available
                .wardType("") // Not available
                .isUrban(determineIfUrban(order.getNewProvinceName()))
                .isMetropolitan(determineIfMetropolitan(order.getNewProvinceName()))
                .isCoastal(false) // Default
                .isBorder(false) // Default
                .economicTier(determineEconomicTier(order.getNewProvinceName()))
                .populationDensity("") // Not available
                .incomeLevel("") // Not available
                .shippingZone(determineShippingZone(order.getNewProvinceName()))
                .deliveryComplexity("NORMAL") // Default
                .standardDeliveryDays(determineDeliveryDays(order.getNewProvinceName()))
                .expressDeliveryAvailable(true) // Default
                .latitude(0.0) // Not available
                .longitude(0.0) // Not available
                .build();
    }

    // Map Facebook order to PaymentInfo entity - CORRECTED
    public PaymentInfo mapToPaymentInfo(FacebookOrderDto order) {
        if (order == null) {
            return null;
        }

        return PaymentInfo.builder()
                .orderId(order.getOrderId())
                .paymentKey(generatePaymentKey(order.getOrderId()))
                .paymentMethod(determinePaymentMethod(order))
                .paymentCategory(order.isCodOrder() ? "CASH_ON_DELIVERY" : "ONLINE_PAYMENT")
                .paymentProvider("FACEBOOK_PAY")
                .isCod(order.isCodOrder())
                .isPrepaid(!order.isCodOrder())
                .isInstallment(false) // Default
                .installmentMonths(0)
                .supportsRefund(true) // Default
                .supportsPartialRefund(true) // Default
                .refundProcessingDays(7) // Default
                .riskLevel("LOW") // Default
                .requiresVerification(false) // Default
                .fraudScore(0.0) // Default
                .transactionFeeRate(0.0) // Default
                .processingFee(0.0) // Default
                .paymentProcessingTimeMinutes(5) // Default
                .settlementDays(1) // Default
                .build();
    }

    // CORRECTED Helper methods
    private double calculateGrossRevenue(FacebookOrderDto order) {
        // Gross Revenue = Net Revenue + Discount
        double netRevenue = order.getTotalAmountAsDouble(); // total_price_after_sub_discount
        double discount = order.getDiscount() != null ? order.getDiscount().doubleValue() : 0.0;
        return netRevenue + discount;
    }

    private String determinePaymentMethod(FacebookOrderDto order) {
        if (order.isCodOrder()) {
            return "COD";
        }
        if (order.getCash() != null && order.getCash() > 0) {
            return "CASH";
        }
        return "ONLINE_PAYMENT";
    }

    private boolean determineIfUrban(String provinceName) {
        if (provinceName == null) return false;
        String province = provinceName.toLowerCase();
        return province.contains("hà nội") || province.contains("hồ chí minh") ||
                province.contains("đà nẵng") || province.contains("cần thơ");
    }

    private boolean determineIfMetropolitan(String provinceName) {
        if (provinceName == null) return false;
        String province = provinceName.toLowerCase();
        return province.contains("hà nội") || province.contains("hồ chí minh");
    }

    private String determineEconomicTier(String provinceName) {
        if (provinceName == null) return "TIER_3";
        String province = provinceName.toLowerCase();
        if (province.contains("hà nội") || province.contains("hồ chí minh")) {
            return "TIER_1";
        }
        if (province.contains("đà nẵng") || province.contains("cần thơ") ||
                province.contains("hải phòng")) {
            return "TIER_2";
        }
        return "TIER_3";
    }

    private String determineShippingZone(String provinceName) {
        if (provinceName == null) return "ZONE_3";
        String province = provinceName.toLowerCase();
        if (province.contains("hà nội") || province.contains("hồ chí minh")) {
            return "ZONE_1";
        }
        if (province.contains("đà nẵng") || province.contains("cần thơ")) {
            return "ZONE_2";
        }
        return "ZONE_3";
    }

    private int determineDeliveryDays(String provinceName) {
        if (provinceName == null) return 5;
        String province = provinceName.toLowerCase();
        if (province.contains("hà nội") || province.contains("hồ chí minh")) {
            return 1; // Same day or next day
        }
        if (province.contains("đà nẵng") || province.contains("cần thơ")) {
            return 2;
        }
        return 3; // Standard delivery
    }

    // Keep existing helper methods unchanged
    private Long generateCustomerKey(String customerId) {
        return Math.abs(customerId.hashCode()) % 1000000000L;
    }

    private String hashPhone(String phone) {
        return phone != null ? "HASH_" + Math.abs(phone.hashCode()) : null;
    }

    private String hashEmail(String email) {
        return email != null ? "HASH_" + Math.abs(email.hashCode()) : null;
    }

    private String normalizeGender(String gender) {
        if (gender == null) return "unknown";
        return gender.toLowerCase().equals("male") || gender.toLowerCase().equals("female") ?
                gender.toLowerCase() : "unknown";
    }

    private double calculateAOV(FacebookCustomer customer) {
        if (customer.getOrderCount() != null && customer.getOrderCount() > 0) {
            return customer.getPurchasedAmountAsDouble() / customer.getOrderCount();
        }
        return 0.0;
    }

    private int calculateDaysSinceFirst(String insertedAt) {
        LocalDateTime firstDate = parseDateTime(insertedAt);
        if (firstDate != null) {
            return (int) java.time.Duration.between(firstDate, LocalDateTime.now()).toDays();
        }
        return 0;
    }

    private int calculateDaysSinceLast(String lastOrderAt) {
        LocalDateTime lastDate = parseDateTime(lastOrderAt);
        if (lastDate != null) {
            return (int) java.time.Duration.between(lastDate, LocalDateTime.now()).toDays();
        }
        return 0;
    }

    private double calculateReturnRate(FacebookCustomer customer) {
        if (customer.getOrderCount() != null && customer.getOrderCount() > 0 &&
                customer.getReturnedOrderCount() != null) {
            return (double) customer.getReturnedOrderCount() / customer.getOrderCount() * 100;
        }
        return 0.0;
    }

    private String extractPrimaryProvince(FacebookOrderDto order) {
        return order.getNewProvinceName() != null ? order.getNewProvinceName() : "";
    }

    private String generateInternalUuid(FacebookOrderDto order) {
        return "FB_" + order.getId() + "_" + System.currentTimeMillis();
    }

    private int calculateItemQuantity(List<FacebookItemDto> items) {
        if (items == null) return 0;
        return items.stream().mapToInt(item -> item.getQuantityOrDefault()).sum();
    }

    private int calculateTotalWeight(List<FacebookItemDto> items) {
        if (items == null) return 0;
        return items.stream()
                .mapToInt(item -> (item.getWeightGram() != null ? item.getWeightGram() : 0) *
                        item.getQuantityOrDefault())
                .sum();
    }

    private boolean isOrderDelivered(Integer status) {
        return status != null && status == 4; // Assuming 4 = delivered
    }

    private boolean isOrderCancelled(Integer status) {
        return status != null && status == -1; // Assuming -1 = cancelled
    }

    private boolean isNewCustomer(FacebookCustomer customer) {
        return customer != null && customer.getOrderCount() != null && customer.getOrderCount() <= 1;
    }

    private double calculateShippingCostRatio(FacebookOrderDto order) {
        double total = order.getTotalAmountAsDouble();
        double shipping = order.getShippingFee() != null ? order.getShippingFee().doubleValue() : 0.0;
        return total > 0 ? (shipping / total) * 100 : 0.0;
    }

    private String calculatePriceRange(FacebookItemDto item) {
        double price = item.getPriceAsDouble();
        if (price < 100000) return "UNDER_100K";
        if (price < 500000) return "100K_500K";
        if (price < 1000000) return "500K_1M";
        return "OVER_1M";
    }

    private Long generateGeographyKey(String orderId) {
        return Math.abs(orderId.hashCode()) % 1000000000L;
    }

    private Long generatePaymentKey(String orderId) {
        return Math.abs(("PAY_" + orderId).hashCode()) % 1000000000L;
    }

    private LocalDateTime parseDateTime(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) {
            return null;
        }

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDateTime.parse(dateTime.trim(), formatter);
            } catch (DateTimeParseException ignored) {
                // Try next formatter
            }
        }

        log.warn("Unable to parse datetime: {}", dateTime);
        return null;
    }
}