package com.guno.dataimport.processor;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.internal.ErrorReport;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.mapper.FacebookMapper;
import com.guno.dataimport.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * BatchProcessor - FINAL: Pure temp table approach, NO DELETE operations
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final FacebookMapper facebookMapper;
    private final CustomerRepository customerRepository;
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    private final GeographyRepository geographyRepository;
    private final PaymentRepository paymentRepository;
    private final ShippingRepository shippingRepository;
    private final ProcessingDateRepository processingDateRepository;

    /**
     * MAIN ENTRY POINT: Process with temp table strategy
     */
    @Transactional
    public ProcessingResult processCollectedData(CollectedData collectedData) {
        log.info("Starting TEMP TABLE batch processing (no delete operations)");

        ProcessingResult result = ProcessingResult.builder()
                .processedAt(LocalDateTime.now())
                .build();

        long startTime = System.currentTimeMillis();

        try {
            ProcessingResult facebookResult = processFacebookOrders(collectedData.getFacebookOrders());

            result.setTotalProcessed(facebookResult.getTotalProcessed());
            result.setSuccessCount(facebookResult.getSuccessCount());
            result.setFailedCount(facebookResult.getFailedCount());
            result.getErrors().addAll(facebookResult.getErrors());

        } catch (Exception e) {
            log.error("Temp table processing error: {}", e.getMessage(), e);
            result.setFailedCount(result.getTotalProcessed());
            result.getErrors().add(ErrorReport.of("BATCH", "ALL", "FACEBOOK", e));
        }

        result.setProcessingTimeMs(System.currentTimeMillis() - startTime);
        log.info("TEMP TABLE processing completed - Success: {}, Failed: {}, Duration: {}ms",
                result.getSuccessCount(), result.getFailedCount(), result.getProcessingTimeMs());

        return result;
    }

    /**
     * Process Facebook orders with temp table strategy
     */
    public ProcessingResult processFacebookOrders(List<Object> facebookOrderObjects) {
        if (facebookOrderObjects == null || facebookOrderObjects.isEmpty()) {
            return ProcessingResult.builder().build();
        }

        log.info("Processing {} Facebook orders with TEMP TABLE strategy", facebookOrderObjects.size());

        ProcessingResult result = ProcessingResult.builder()
                .totalProcessed(facebookOrderObjects.size())
                .build();

        List<FacebookOrderDto> facebookOrders = facebookOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        try {
            // Map all entities
            List<Customer> customers = mapCustomers(facebookOrders, result);
            List<Order> orders = mapOrders(facebookOrders, result);
            List<OrderItem> orderItems = mapOrderItems(facebookOrders, result);
            List<Product> products = mapProducts(facebookOrders, result);
            List<GeographyInfo> geography = mapGeography(facebookOrders, result);
            List<PaymentInfo> payments = mapPayments(facebookOrders, result);
            List<ProcessingDateInfo> dates = mapDates(facebookOrders, result);

            log.info("Mapped entities - Customers: {}, Orders: {}, Items: {}, Products: {}",
                    customers.size(), orders.size(), orderItems.size(), products.size());

            // TEMP TABLE bulk upserts (handles all duplicates automatically)
            log.info("Starting TEMP TABLE upserts for all entities");

            customerRepository.bulkUpsert(customers);
            log.debug("✅ Customers upserted via temp table");

            orderRepository.bulkUpsert(orders);
            log.debug("✅ Orders upserted via temp table");

            productRepository.bulkUpsert(products);
            log.debug("✅ Products upserted via temp table");

            orderItemRepository.bulkUpsert(orderItems);
            log.debug("✅ Order items upserted via temp table");

            geographyRepository.bulkUpsert(geography);
            log.debug("✅ Geography upserted via temp table");

            paymentRepository.bulkUpsert(payments);
            log.debug("✅ Payments upserted via temp table");

            processingDateRepository.bulkUpsert(dates);
            log.debug("✅ Processing dates upserted via temp table");

            result.setSuccessCount(facebookOrders.size() - result.getFailedCount());
            log.info("Successfully processed {} orders via TEMP TABLE strategy", result.getSuccessCount());

        } catch (Exception e) {
            log.error("TEMP TABLE processing error: {}", e.getMessage(), e);
            result.setFailedCount(facebookOrders.size());
            result.getErrors().add(ErrorReport.of("FACEBOOK_ORDERS", "BATCH", "FACEBOOK", e));
        }

        return result;
    }

    // ================================
    // MAPPING METHODS - Stream-based
    // ================================

    private List<Customer> mapCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToCustomer(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("CUSTOMER", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(customer -> customer != null)
                .collect(Collectors.toMap(
                        Customer::getCustomerId,
                        customer -> customer,
                        (existing, replacement) -> existing))
                .values()
                .stream()
                .toList();
    }

    private List<Order> mapOrders(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToOrder(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("ORDER", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(order -> order != null)
                .toList();
    }

    private List<OrderItem> mapOrderItems(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderItem> allItems = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allItems.addAll(facebookMapper.mapToOrderItems(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("ORDER_ITEMS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allItems;
    }

    private List<Product> mapProducts(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Product> allProducts = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allProducts.addAll(facebookMapper.mapToProducts(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("PRODUCTS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        // Remove duplicates
        return allProducts.stream()
                .collect(Collectors.toMap(
                        product -> product.getSku() + "_" + product.getPlatformProductId(),
                        product -> product,
                        (existing, replacement) -> existing))
                .values()
                .stream()
                .toList();
    }

    private List<GeographyInfo> mapGeography(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToGeographyInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("GEOGRAPHY", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(geo -> geo != null)
                .toList();
    }

    private List<PaymentInfo> mapPayments(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToPaymentInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("PAYMENT", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(payment -> payment != null)
                .toList();
    }

    private List<ProcessingDateInfo> mapDates(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return createDateInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("DATE_INFO", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    /**
     * Create date info from order
     */
    private ProcessingDateInfo createDateInfo(FacebookOrderDto order) {
        if (order.getCreatedAt() == null) return null;

        try {
            LocalDateTime createdAt = LocalDateTime.parse(order.getCreatedAt().replace("Z", ""));
            return ProcessingDateInfo.builder()
                    .orderId(order.getOrderId())
                    .dateKey(Long.valueOf(createdAt.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))))
                    .fullDate(LocalDateTime.parse(order.getCreatedAt().replace("Z", "")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                    .dayOfWeek(createdAt.getDayOfWeek().getValue())
                    .dayOfWeekName(createdAt.getDayOfWeek().name())
                    .dayOfMonth(createdAt.getDayOfMonth())
                    .monthOfYear(createdAt.getMonthValue())
                    .monthName(createdAt.getMonth().name())
                    .year(createdAt.getYear())
                    .isWeekend(createdAt.getDayOfWeek().getValue() >= 6)
                    .isBusinessDay(createdAt.getDayOfWeek().getValue() <= 5)
                    .build();
        } catch (Exception e) {
            log.warn("Date parsing failed for order {}: {}", order.getOrderId(), e.getMessage());
            return null;
        }
    }
}