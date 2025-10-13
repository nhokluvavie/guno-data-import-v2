package com.guno.dataimport.processor;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ErrorReport;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

/**
 * ValidationProcessor - Optimized data validation before processing
 * Validates orders from all platforms (Facebook, TikTok, Shopee)
 */
@Component
@Slf4j
public class ValidationProcessor {

    /**
     * Validate all collected data
     */
    public List<ErrorReport> validateCollectedData(CollectedData data) {
        if (data == null) {
            return List.of(error("COLLECTED_DATA", "NULL", "Data is null"));
        }

        log.info("Validating {} total orders (FB: {}, TT: {}, SP: {})",
                data.getTotalOrders(),
                data.getFacebookOrders().size(),
                data.getTikTokOrders().size(),
                data.getShopeeOrders().size());

        List<ErrorReport> errors = new ArrayList<>();
        errors.addAll(validateOrders(data.getFacebookOrders(), "FACEBOOK"));
        errors.addAll(validateOrders(data.getTikTokOrders(), "TIKTOK"));
        errors.addAll(validateOrders(data.getShopeeOrders(), "SHOPEE"));

        log.info("Validation completed: {} errors found", errors.size());
        return errors;
    }

    /**
     * Validate orders for any platform (reusable)
     */
    private List<ErrorReport> validateOrders(List<Object> orders, String platform) {
        if (orders == null || orders.isEmpty()) {
            return List.of();
        }

        List<ErrorReport> errors = new ArrayList<>();

        for (int i = 0; i < orders.size(); i++) {
            try {
                Object obj = orders.get(i);

                if (!(obj instanceof FacebookOrderDto)) {
                    errors.add(error(platform + "_ORDER", String.valueOf(i),
                            "Invalid type: " + obj.getClass().getSimpleName()));
                    continue;
                }

                errors.addAll(validateOrder((FacebookOrderDto) obj, platform));

            } catch (Exception e) {
                errors.add(error(platform + "_ORDER", String.valueOf(i),
                        "Validation error: " + e.getMessage()));
            }
        }

        return errors;
    }

    /**
     * Validate single order - orchestrates all validations
     */
    private List<ErrorReport> validateOrder(FacebookOrderDto order, String platform) {
        List<ErrorReport> errors = new ArrayList<>();
        String orderId = order.getOrderId();

        // Basic order validation
        if (isBlank(orderId)) {
            return List.of(error("ORDER", "UNKNOWN", "Order ID missing", platform));
        }

        // Core validations
        errors.addAll(validateCustomer(order, orderId, platform));
        errors.addAll(validateItems(order, orderId, platform));
        errors.addAll(validateFinancials(order, orderId, platform));
        errors.addAll(validateGeography(order, orderId, platform));
        errors.addAll(validateDates(order, orderId, platform));

        return errors;
    }

    // ========== ENTITY-SPECIFIC VALIDATIONS ==========

    private List<ErrorReport> validateCustomer(FacebookOrderDto order, String orderId, String platform) {
        if (order.getCustomer() == null) {
            return List.of(error("CUSTOMER", orderId, "Customer missing", platform));
        }

        if (isBlank(order.getCustomer().getId())) {
            return List.of(error("CUSTOMER", orderId, "Customer ID missing", platform));
        }

        return List.of();
    }

    private List<ErrorReport> validateItems(FacebookOrderDto order, String orderId, String platform) {
        List<ErrorReport> errors = new ArrayList<>();

        if (order.getItems() == null || order.getItems().isEmpty()) {
            errors.add(error("ORDER_ITEM", orderId, "No items in order", platform));
            return errors;
        }

        for (int i = 0; i < order.getItems().size(); i++) {
            var item = order.getItems().get(i);
            String prefix = "Item[" + i + "]: ";

            if (item.getId() == null) {
                errors.add(error("ORDER_ITEM", orderId, prefix + "ID missing", platform));
            }
            if (item.getQuantity() == null || item.getQuantity() <= 0) {
                errors.add(error("ORDER_ITEM", orderId, prefix + "Invalid quantity", platform));
            }
            if (item.getPriceAsDouble() == null || item.getPriceAsDouble() < 0) {
                errors.add(error("ORDER_ITEM", orderId, prefix + "Invalid price", platform));
            }
        }

        return errors;
    }

    private List<ErrorReport> validateFinancials(FacebookOrderDto order, String orderId, String platform) {
        List<ErrorReport> errors = new ArrayList<>();

        if (order.getTotalPriceAfterSubDiscount() != null &&
                order.getTotalPriceAfterSubDiscount() < 0) {
            errors.add(error("FINANCIAL", orderId, "Negative total price", platform));
        }

        if (order.getShippingFee() != null && order.getShippingFee() < 0) {
            errors.add(error("FINANCIAL", orderId, "Negative shipping fee", platform));
        }

        if (order.isCodOrder()) {
            Long codAmount = order.getCod();
            if (codAmount == null || codAmount <= 0) {
                errors.add(error("FINANCIAL", orderId, "Invalid COD amount", platform));
            }
        }

        return errors;
    }

    private List<ErrorReport> validateGeography(FacebookOrderDto order, String orderId, String platform) {
        // Check if data and shipping address exist
        if (order.getData() == null || order.getData().getShippingAddress() == null) {
            return List.of(error("GEOGRAPHY", orderId, "Shipping address missing", platform));
        }

        var shippingAddress = order.getData().getShippingAddress();

        // Province name is required for geography mapping
        if (isBlank(shippingAddress.getProvinceName())) {
            return List.of(error("GEOGRAPHY", orderId, "Province name missing (required)", platform));
        }

        // District missing is just a warning (log only)
        if (isBlank(shippingAddress.getDistrictName())) {
            log.debug("Order {} missing district name", orderId);
        }

        return List.of();
    }

    private List<ErrorReport> validateDates(FacebookOrderDto order, String orderId, String platform) {
        if (isBlank(String.valueOf(order.getCreatedAt()))) {
            return List.of(error("DATE", orderId, "Creation date missing", platform));
        }

        return List.of();
    }

    // ========== UTILITY METHODS ==========

    /**
     * Check if data is valid (no critical errors)
     */
    public boolean isDataValid(CollectedData data) {
        List<ErrorReport> errors = validateCollectedData(data);

        long criticalErrors = errors.stream()
                .filter(e -> e.getErrorMessage().contains("missing") ||
                        e.getErrorMessage().contains("Invalid"))
                .count();

        boolean valid = criticalErrors == 0;
        log.info("Validation result: {} (Critical: {}, Total: {})",
                valid ? "VALID" : "INVALID", criticalErrors, errors.size());

        return valid;
    }

    /**
     * Get validation summary
     */
    public String getValidationSummary(List<ErrorReport> errors) {
        if (errors.isEmpty()) {
            return "✅ No validation errors";
        }

        long critical = errors.stream()
                .filter(e -> e.getErrorMessage().contains("missing") ||
                        e.getErrorMessage().contains("Invalid"))
                .count();

        long warnings = errors.size() - critical;

        return String.format("⚠️ Validation: %d critical, %d warnings, %d total",
                critical, warnings, errors.size());
    }

    // ========== HELPER METHODS ==========

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    private ErrorReport error(String type, String id, String message) {
        return error(type, id, message, "UNKNOWN");
    }

    private ErrorReport error(String type, String id, String message, String platform) {
        return ErrorReport.builder()
                .entityType(type)
                .entityId(id)
                .platform(platform)
                .errorCode("VALIDATION_ERROR")
                .errorMessage(message)
                .build();
    }
}