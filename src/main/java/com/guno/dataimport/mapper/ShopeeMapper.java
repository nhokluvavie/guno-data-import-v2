package com.guno.dataimport.mapper;

import com.guno.dataimport.dto.platform.facebook.AdvancedPlatformFee;
import com.guno.dataimport.dto.platform.facebook.FacebookCustomer;
import com.guno.dataimport.dto.platform.facebook.FacebookItemDto;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.util.GeographyHelper;
import com.guno.dataimport.util.KeyGenerator;
import com.guno.dataimport.util.OrderStatusValidator;
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
 * Shopee Mapper - Convert Facebook DTOs to Database Entities
 * IMPROVED: Better null handling, more accurate mapping, simplified logic
 */
@Component
@Slf4j
public class ShopeeMapper {
    @Value("${api.shopee.default-date:}")
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    // ================================
    // CUSTOMER MAPPING
    // ================================

    public Customer mapToCustomer(FacebookOrderDto order) {
        if (order == null) return null;

        if (order.getCustomer() == null) {
            return Customer.builder()
                    .customerId("GUEST_" + order.getOrderId())
                    .customerKey(0L)
                    .platformCustomerId("GUEST")
                    .customerSegment("GUEST")
                    .customerTier("GUEST")
                    .acquisitionChannel("SHOPEE")
                    .preferredPlatform("SHOPEE")
                    .build();
        }

        FacebookCustomer fbCustomer = order.getCustomer();
        String customerId = fbCustomer.getCustomerId() != null ? fbCustomer.getCustomerId() : fbCustomer.getId();

        return Customer.builder()
                .customerId(customerId)
                .customerKey(generateKey(customerId))
                .platformCustomerId(customerId)
                .phoneHash(fbCustomer.getPrimaryPhone())  // Keep original phone
                .emailHash(fbCustomer.getPrimaryEmail())  // Keep original email
                .gender(fbCustomer.getGender())
                .ageGroup("")
                .customerSegment("SHOPEE")
                .customerTier("STANDARD")
                .acquisitionChannel("SHOPEE")
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
                .favoriteCategory("")
                .favoriteBrand("")
                .preferredPaymentMethod("")
                .preferredPlatform("SHOPEE")
                .primaryShippingProvince("")
                .shipsToMultipleProvinces(false)
                .loyaltyPoints(safeInt(fbCustomer.getRewardPoint()))
                .referralCount(safeInt(fbCustomer.getCountReferrals()))
                .isReferrer(safeBool(fbCustomer.getIsReferrer()))
                .customerName(fbCustomer.getName())
                .build();
    }

    // ================================
    // ORDER MAPPING
    // ================================

    public Order mapToOrder(FacebookOrderDto order) {
        if (order == null) return null;

        return Order.builder()
                .orderId(order.getOrderId())
                .customerId(extractCustomerId(order))
                .shopId(order.getAccountName())
                .internalUuid("SHOPEE")
                .orderCount(1)
                .itemQuantity(calculateTotalQuantity(order))
                .totalItemsInOrder(order.getItems().size())
                .grossRevenue(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .netRevenue(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .shippingFee(safeDouble(order.getShippingFee()))
                .taxAmount(safeDouble(order.getTax()))
                .discountAmount(safeDouble(order.getDiscount()))
                .codAmount(safeDouble(order.getCod()))
                .platformFee(calculatePlatformFee(order))
                .sellerDiscount(0.0)
                .platformDiscount(safeDouble(order.getDiscount()))
                .originalPrice(safeDouble(order.getTotalPriceAfterSubDiscount()))
                .estimatedShippingFee(safeDouble(order.getShippingFee()))
                .actualShippingFee(safeDouble(order.getShippingFee()))
                .shippingWeightGram(0)
                .daysToShip(0)
                .isDelivered(OrderStatusValidator.isDelivered(order, "SHOPEE"))
                .isCancelled(OrderStatusValidator.isCancelled(order, "SHOPEE"))
                .isReturned(OrderStatusValidator.isReturned(order, "SHOPEE"))
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
                .createdAt(order.getCreatedAt())
                .orderSource("UNKNOWN")
                .platformSpecificData(0)
                .sellerId(extractSellerId(order))
                .sellerName(extractSellerName(order))
                .sellerEmail(extractSellerEmail(order))
                .build();
    }

    private boolean isOrderReturned(FacebookOrderDto order) {
        if (order == null || order.getTrackingHistories() == null) {
            return false;
        }
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
                    .platformProductId("SP_" + item.getId())
                    .quantity(qty)
                    .unitPrice(price)
                    .totalPrice(price * qty)
                    .itemDiscount(safeDouble(item.getTotalDiscount()))
                    .promotionType(null)
                    .promotionCode(null)
                    .itemStatus(null)
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
                    .platformProductId("SP_" + item.getId())
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
                    .skuGroup(getSkuGroup(item))
                    .build());
        }
        return products;
    }

    // ================================
    // GEOGRAPHY MAPPING
    // ================================

    public GeographyInfo mapToGeographyInfo(FacebookOrderDto order) {
        if (order == null) return null;

        String province = order.getProvinceSafe();
        String district = order.getDistrictSafe();

        return GeographyInfo.builder()
                .orderId(order.getOrderId())
                .geographyKey(KeyGenerator.generateGeographyKey(province, district))
                .countryCode("VN")
                .countryName("Vietnam")
                .provinceName(province)
                .districtName(district)
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
                .wardName("")
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

    public PaymentInfo mapToPaymentInfo(FacebookOrderDto order) {
        if (order == null) return null;

        boolean isCod = order.isCodOrder();
        String method = isCod ? "COD" : "ONLINE";
        String provider = isCod ? "CASH" : "SHOPEE_PAY";
        String category = isCod ? "CASH_ON_DELIVERY" : "ONLINE_PAYMENT";

        return PaymentInfo.builder()
                .orderId(order.getOrderId())
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

    public ShippingInfo mapToShippingInfo(FacebookOrderDto order) {
        if (order == null) return null;

        String providerId = "SHOPEE_LOGISTICS";
        String serviceType = "STANDARD";

        return ShippingInfo.builder()
                .orderId(order.getOrderId())
                .shippingKey(KeyGenerator.generateShippingKey(providerId, serviceType))
                .providerId(providerId)
                .providerName("Shopee Logistics")
                .providerType("MARKETPLACE")
                .providerTier("STANDARD")
                .serviceType(serviceType)
                .serviceTier("STANDARD")
                .deliveryCommitment("")
                .shippingMethod("STANDARD")
                .pickupType("")
                .deliveryType("")
                .baseFee(safeDouble(order.getShippingFee()))
                .weightBasedFee(0.0)
                .distanceBasedFee(0.0)
                .codFee(0.0)
                .insuranceFee(0.0)
                .supportsCod(order.isCodOrder())
                .supportsInsurance(false)
                .supportsFragile(false)
                .supportsRefrigerated(false)
                .providesTracking(false)
                .providesSmsUpdates(false)
                .averageDeliveryDays(0.0)
                .onTimeDeliveryRate(0.0)
                .successDeliveryRate(0.0)
                .damageRate(0.0)
                .coverageProvinces(getProvinceName(order))
                .coverageNationwide(false)
                .coverageInternational(false)
                .build();
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
                    .transitionTimestamp(order.getInsertedAt())
                    .durationInPreviousStatusHours(0)
                    .transitionReason("ORDER_CREATED")
                    .transitionTrigger("SYSTEM")
                    .changedBy("SHOPEE_API")
                    .isOnTimeTransition(true)
                    .isExpectedTransition(true)
                    .historyKey(generateKey("HIST_" + order.getOrderId()))
                    .createdAt(String.valueOf(order.getInsertedAt()))
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

        LocalDateTime orderDate = order.getInsertedAt();
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

    private String extractSellerId(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getId() != null) {
            return order.getAssigningSeller().getId();
        }
        return "UNKNOWN";
    }

    private String extractSellerName(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getName() != null) {
            return order.getAssigningSeller().getName();
        }
        if (order.getAccountName() != null && !order.getAccountName().trim().isEmpty()) {
            return order.getAccountName().trim();
        }
        return "UNKNOWN";
    }

    private String extractSellerEmail(FacebookOrderDto order) {
        if (order.getAssigningSeller() != null && order.getAssigningSeller().getEmail() != null) {
            return order.getAssigningSeller().getEmail();
        }
        return "UNKNOWN";
    }

    // ================================
    // HELPER METHODS - STATUS
    // ================================

    private String extractSubStatusId(FacebookOrderDto order) {
        Integer subStatus = order.getSubStatus();
        return subStatus != null ? subStatus.toString() : "0";
    }

    private Integer extractPartnerStatusId(FacebookOrderDto order) {
        if (order.getTrackingHistories() == null || order.getTrackingHistories().isEmpty()) {
            return 0;
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

    // ================================
    // HELPER METHODS - ITEM/PRODUCT (FIXED FOR DTO STRUCTURE)
    // ================================

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
        if (item.getVariationInfo() == null || item.getVariationInfo().getFields() == null) {
            return null;
        }
        return item.getVariationInfo().getFields().stream()
                .filter(f -> fieldName.equals(f.getName()))
                .map(FacebookItemDto.VariationField::getValue)
                .findFirst()
                .orElse(null);
    }

    private int getWeight(FacebookItemDto item) {
        if (item.getVariationInfo() != null && item.getVariationInfo().getWeight() != null) {
            return item.getVariationInfo().getWeight();
        }
        return 0;
    }

    private String getPriceRange(double price) {
        if (price < 100000) return "UNDER_100K";
        if (price < 500000) return "100K_500K";
        if (price < 1000000) return "500K_1M";
        return "OVER_1M";
    }

    private String getImageUrl(FacebookItemDto item) {
        if (item.getVariationInfo() != null && item.getVariationInfo().getImages() != null) {
            List<String> images = item.getVariationInfo().getImages();
            if (!images.isEmpty()) {
                return images.get(0);
            }
        }
        return null;
    }

    private int getImageCount(FacebookItemDto item) {
        if (item.getVariationInfo() != null && item.getVariationInfo().getImages() != null) {
            return item.getVariationInfo().getImages().size();
        }
        return 0;
    }

    // ================================
    // HELPER METHODS - GEOGRAPHY
    // ================================

    private String getProvinceName(FacebookOrderDto order) {
        String province = order.getNewProvinceName();
        return province != null && !province.trim().isEmpty() ? province.trim() : "Unknown";
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
        return "GUEST_" + order.getOrderId();
    }

    private int calculateTotalQuantity(FacebookOrderDto order) {
        return order.getItems().stream()
                .mapToInt(item -> safeInt(item.getQuantity()))
                .sum();
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

    private String getSkuGroup(FacebookItemDto item) {
        if (item.getVariationInfo() != null) {
            String productDisplayId = item.getVariationInfo().getProductDisplayId();
            if (productDisplayId != null && !productDisplayId.trim().isEmpty()) {
                return productDisplayId.trim();
            }
        }
        return null;
    }

    private double calculatePlatformFee(FacebookOrderDto order) {
        if (order.getData() == null ||
                order.getData().getAdvancedPlatformFee() == null) {
            return 0.0;
        }

        AdvancedPlatformFee fees = order.getData().getAdvancedPlatformFee();
        double total = 0.0;

        total += safeDouble(fees.getTax());
        total += safeDouble(fees.getPaymentFee());
        total += safeDouble(fees.getServiceFee());
        total += safeDouble(fees.getSellerTransactionFee());

        return total;
    }
}