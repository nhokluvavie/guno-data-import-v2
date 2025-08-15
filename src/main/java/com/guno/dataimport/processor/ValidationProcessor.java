package com.guno.dataimport.processor;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ErrorReport;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

/**
 * ValidationProcessor - Validates data before processing
 */
@Component
@Slf4j
public class ValidationProcessor {

    /**
     * Validate collected data
     */
    public List<ErrorReport> validateCollectedData(CollectedData collectedData) {
        List<ErrorReport> errors = new ArrayList<>();

        if (collectedData == null) {
            errors.add(createValidationError("COLLECTED_DATA", "NULL", "Data is null"));
            return errors;
        }

        log.info("Validating collected data with {} total orders", collectedData.getTotalOrders());

        // Validate Facebook orders
        errors.addAll(validateFacebookOrders(collectedData.getFacebookOrders()));

        log.info("Validation completed with {} errors", errors.size());
        return errors;
    }

    /**
     * Validate Facebook orders
     */
    private List<ErrorReport> validateFacebookOrders(List<Object> facebookOrderObjects) {
        List<ErrorReport> errors = new ArrayList<>();

        if (facebookOrderObjects == null || facebookOrderObjects.isEmpty()) {
            log.info("No Facebook orders to validate");
            return errors;
        }

        log.info("Validating {} Facebook orders", facebookOrderObjects.size());

        for (int i = 0; i < facebookOrderObjects.size(); i++) {
            try {
                Object orderObj = facebookOrderObjects.get(i);

                if (!(orderObj instanceof FacebookOrderDto)) {
                    errors.add(createValidationError("FACEBOOK_ORDER", String.valueOf(i),
                            "Invalid order type: " + orderObj.getClass().getSimpleName()));
                    continue;
                }

                FacebookOrderDto order = (FacebookOrderDto) orderObj;
                errors.addAll(validateFacebookOrder(order));

            } catch (Exception e) {
                errors.add(createValidationError("FACEBOOK_ORDER", String.valueOf(i),
                        "Validation error: " + e.getMessage()));
            }
        }

        return errors;
    }

    /**
     * Validate individual Facebook order
     */
    private List<ErrorReport> validateFacebookOrder(FacebookOrderDto order) {
        List<ErrorReport> errors = new ArrayList<>();
        String orderId = order.getOrderId();

        // Required fields validation
        if (orderId == null || orderId.trim().isEmpty()) {
            errors.add(createValidationError("FACEBOOK_ORDER", "UNKNOWN", "Order ID is missing"));
            return errors; // Cannot continue without order ID
        }

        // Customer validation
        if (order.getCustomer() == null) {
            errors.add(createValidationError("FACEBOOK_ORDER", orderId, "Customer is missing"));
        } else {
            if (order.getCustomer().getId() == null || order.getCustomer().getId().trim().isEmpty()) {
                errors.add(createValidationError("FACEBOOK_ORDER", orderId, "Customer ID is missing"));
            }
        }

        // Financial data validation
        if (order.getTotalPriceAfterSubDiscount() != null && order.getTotalPriceAfterSubDiscount() < 0) {
            errors.add(createValidationError("FACEBOOK_ORDER", orderId, "Negative total price"));
        }

        if (order.getShippingFee() != null && order.getShippingFee() < 0) {
            errors.add(createValidationError("FACEBOOK_ORDER", orderId, "Negative shipping fee"));
        }

        // Items validation
        if (order.getItems() == null || order.getItems().isEmpty()) {
            errors.add(createValidationError("FACEBOOK_ORDER", orderId, "No items in order"));
        } else {
            for (int i = 0; i < order.getItems().size(); i++) {
                var item = order.getItems().get(i);
                if (item.getId() == null) {
                    errors.add(createValidationError("FACEBOOK_ORDER", orderId,
                            "Item " + i + " has no ID"));
                }
                if (item.getQuantity() == null || item.getQuantity() <= 0) {
                    errors.add(createValidationError("FACEBOOK_ORDER", orderId,
                            "Item " + i + " has invalid quantity"));
                }
                if (item.getPriceAsDouble() == null || item.getPriceAsDouble() < 0) {
                    errors.add(createValidationError("FACEBOOK_ORDER", orderId,
                            "Item " + i + " has invalid price"));
                }
            }
        }

        // Date validation
        if (order.getCreatedAt() == null || order.getCreatedAt().trim().isEmpty()) {
            errors.add(createValidationError("FACEBOOK_ORDER", orderId, "Creation date is missing"));
        }

        return errors;
    }

    /**
     * Check if data is valid (no critical errors)
     */
    public boolean isDataValid(CollectedData collectedData) {
        List<ErrorReport> errors = validateCollectedData(collectedData);

        // Count critical errors (missing required fields)
        long criticalErrors = errors.stream()
                .filter(error -> error.getErrorMessage().contains("missing") ||
                        error.getErrorMessage().contains("null") ||
                        error.getErrorMessage().contains("No items"))
                .count();

        boolean isValid = criticalErrors == 0;
        log.info("Data validation result: {} (Critical errors: {})",
                isValid ? "VALID" : "INVALID", criticalErrors);

        return isValid;
    }

    /**
     * Get validation summary
     */
    public String getValidationSummary(List<ErrorReport> errors) {
        if (errors.isEmpty()) {
            return "No validation errors found";
        }

        long criticalErrors = errors.stream()
                .filter(error -> error.getErrorMessage().contains("missing") ||
                        error.getErrorMessage().contains("null"))
                .count();

        long warningErrors = errors.size() - criticalErrors;

        return String.format("Validation Summary - Critical: %d, Warnings: %d, Total: %d",
                criticalErrors, warningErrors, errors.size());
    }

    private ErrorReport createValidationError(String entityType, String entityId, String message) {
        return ErrorReport.builder()
                .entityType(entityType)
                .entityId(entityId)
                .platform("FACEBOOK")
                .errorCode("VALIDATION_ERROR")
                .errorMessage(message)
                .build();
    }
}