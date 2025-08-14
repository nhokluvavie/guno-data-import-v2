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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * BatchProcessor - Main processor for mapping and saving data to database
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final FacebookMapper facebookMapper;

    // Repositories
    private final CustomerRepository customerRepository;
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    private final GeographyRepository geographyRepository;
    private final PaymentRepository paymentRepository;
    private final ShippingRepository shippingRepository;
    private final ProcessingDateRepository processingDateRepository;

    /**
     * Process collected data - main entry point
     */
    @Transactional
    public ProcessingResult processCollectedData(CollectedData collectedData) {
        log.info("Starting batch processing of collected data");

        ProcessingResult result = ProcessingResult.builder()
                .processedAt(LocalDateTime.now())
                .build();

        long startTime = System.currentTimeMillis();

        try {
            // Process Facebook orders
            ProcessingResult facebookResult = processFacebookOrders(collectedData.getFacebookOrders());

            // Aggregate results
            result.setTotalProcessed(facebookResult.getTotalProcessed());
            result.setSuccessCount(facebookResult.getSuccessCount());
            result.setFailedCount(facebookResult.getFailedCount());
            result.getErrors().addAll(facebookResult.getErrors());

        } catch (Exception e) {
            log.error("Error during batch processing: {}", e.getMessage(), e);
            result.setFailedCount(result.getTotalProcessed());
            result.getErrors().add(ErrorReport.of("BATCH", "ALL", "FACEBOOK", e));
        }

        result.setProcessingTimeMs(System.currentTimeMillis() - startTime);

        log.info("Batch processing completed - Success: {}, Failed: {}, Duration: {}ms",
                result.getSuccessCount(), result.getFailedCount(), result.getProcessingTimeMs());

        return result;
    }

    /**
     * Process Facebook orders specifically
     */
    private ProcessingResult processFacebookOrders(List<Object> facebookOrderObjects) {
        if (facebookOrderObjects == null || facebookOrderObjects.isEmpty()) {
            return ProcessingResult.builder().build();
        }

        log.info("Processing {} Facebook orders", facebookOrderObjects.size());

        ProcessingResult result = ProcessingResult.builder()
                .totalProcessed(facebookOrderObjects.size())
                .build();

        List<FacebookOrderDto> facebookOrders = facebookOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        try {
            // Step 1: Process customers
            List<Customer> customers = mapAndSaveCustomers(facebookOrders, result);

            // Step 2: Process orders
            List<Order> orders = mapAndSaveOrders(facebookOrders, result);

            // Step 3: Process order items and products
            processOrderItemsAndProducts(facebookOrders, result);

            // Step 4: Process supporting data
            processSupportingData(facebookOrders, result);

            result.setSuccessCount(facebookOrders.size() - result.getFailedCount());

        } catch (Exception e) {
            log.error("Error processing Facebook orders: {}", e.getMessage(), e);
            result.setFailedCount(facebookOrders.size());
            result.getErrors().add(ErrorReport.of("FACEBOOK_ORDERS", "BATCH", "FACEBOOK", e));
        }

        return result;
    }

    /**
     * Map and save customers
     */
    private List<Customer> mapAndSaveCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        log.info("Processing customers from {} orders", orders.size());

        List<Customer> customers = new ArrayList<>();

        for (FacebookOrderDto order : orders) {
            try {
                Customer customer = facebookMapper.mapToCustomer(order);
                if (customer != null) {
                    customers.add(customer);
                }
            } catch (Exception e) {
                log.warn("Failed to map customer for order {}: {}", order.getOrderId(), e.getMessage());
                result.getErrors().add(ErrorReport.of("CUSTOMER", order.getOrderId(), "FACEBOOK", e));
            }
        }

        // Remove duplicates by customer ID
        customers = customers.stream()
                .collect(Collectors.toMap(
                        Customer::getCustomerId,
                        customer -> customer,
                        (existing, replacement) -> existing))
                .values()
                .stream()
                .toList();

        try {
            int savedCount = customerRepository.bulkUpsert(customers);
            log.info("Saved {} unique customers", savedCount);
        } catch (Exception e) {
            log.error("Failed to save customers: {}", e.getMessage(), e);
            result.getErrors().add(ErrorReport.of("CUSTOMER_BATCH", "ALL", "FACEBOOK", e));
        }

        return customers;
    }

    /**
     * Map and save orders
     */
    private List<Order> mapAndSaveOrders(List<FacebookOrderDto> orderDtos, ProcessingResult result) {
        log.info("Processing {} orders", orderDtos.size());

        List<Order> orders = new ArrayList<>();

        for (FacebookOrderDto orderDto : orderDtos) {
            try {
                Order order = facebookMapper.mapToOrder(orderDto);
                if (order != null) {
                    orders.add(order);
                }
            } catch (Exception e) {
                log.warn("Failed to map order {}: {}", orderDto.getOrderId(), e.getMessage());
                result.getErrors().add(ErrorReport.of("ORDER", orderDto.getOrderId(), "FACEBOOK", e));
            }
        }

        try {
            int savedCount = orderRepository.bulkUpsert(orders);
            log.info("Saved {} orders", savedCount);
        } catch (Exception e) {
            log.error("Failed to save orders: {}", e.getMessage(), e);
            result.getErrors().add(ErrorReport.of("ORDER_BATCH", "ALL", "FACEBOOK", e));
        }

        return orders;
    }

    /**
     * Process order items and products
     */
    private void processOrderItemsAndProducts(List<FacebookOrderDto> orders, ProcessingResult result) {
        log.info("Processing order items and products");

        List<OrderItem> allOrderItems = new ArrayList<>();
        List<Product> allProducts = new ArrayList<>();
        Set<String> orderIds = orders.stream()
                .map(FacebookOrderDto::getOrderId)
                .collect(Collectors.toSet());

        for (FacebookOrderDto order : orders) {
            try {
                // Map order items
                List<OrderItem> orderItems = facebookMapper.mapToOrderItems(order);
                allOrderItems.addAll(orderItems);

                // Map products
                List<Product> products = facebookMapper.mapToProducts(order);
                allProducts.addAll(products);

            } catch (Exception e) {
                log.warn("Failed to map items/products for order {}: {}", order.getOrderId(), e.getMessage());
                result.getErrors().add(ErrorReport.of("ORDER_ITEMS", order.getOrderId(), "FACEBOOK", e));
            }
        }

        try {
            // Save products first (referenced by order items)
            List<Product> uniqueProducts = allProducts.stream()
                    .collect(Collectors.toMap(
                            product -> product.getSku() + "_" + product.getPlatformProductId(),
                            product -> product,
                            (existing, replacement) -> existing))
                    .values()
                    .stream()
                    .toList();

            int savedProducts = productRepository.bulkUpsert(uniqueProducts);
            log.info("Saved {} unique products", savedProducts);

            // Save order items (refresh strategy)
            int savedItems = orderItemRepository.bulkRefresh(orderIds, allOrderItems);
            log.info("Saved {} order items", savedItems);

        } catch (Exception e) {
            log.error("Failed to save order items/products: {}", e.getMessage(), e);
            result.getErrors().add(ErrorReport.of("ITEMS_PRODUCTS_BATCH", "ALL", "FACEBOOK", e));
        }
    }

    /**
     * Process supporting data (geography, payment, shipping, dates)
     */
    private void processSupportingData(List<FacebookOrderDto> orders, ProcessingResult result) {
        log.info("Processing supporting data");

        List<GeographyInfo> geographyInfos = new ArrayList<>();
        List<PaymentInfo> paymentInfos = new ArrayList<>();
        List<ProcessingDateInfo> dateInfos = new ArrayList<>();

        for (FacebookOrderDto order : orders) {
            try {
                // Geography
                GeographyInfo geography = facebookMapper.mapToGeographyInfo(order);
                if (geography != null) {
                    geographyInfos.add(geography);
                }

                // Payment
                PaymentInfo payment = facebookMapper.mapToPaymentInfo(order);
                if (payment != null) {
                    paymentInfos.add(payment);
                }

                // Date info (created from order creation date)
                ProcessingDateInfo dateInfo = createDateInfo(order);
                if (dateInfo != null) {
                    dateInfos.add(dateInfo);
                }

            } catch (Exception e) {
                log.warn("Failed to map supporting data for order {}: {}", order.getOrderId(), e.getMessage());
                result.getErrors().add(ErrorReport.of("SUPPORTING_DATA", order.getOrderId(), "FACEBOOK", e));
            }
        }

        try {
            // Save all supporting data
            geographyRepository.bulkUpsert(geographyInfos);
            paymentRepository.bulkUpsert(paymentInfos);
            processingDateRepository.bulkUpsert(dateInfos);

            log.info("Saved supporting data - Geography: {}, Payment: {}, Dates: {}",
                    geographyInfos.size(), paymentInfos.size(), dateInfos.size());

        } catch (Exception e) {
            log.error("Failed to save supporting data: {}", e.getMessage(), e);
            result.getErrors().add(ErrorReport.of("SUPPORTING_DATA_BATCH", "ALL", "FACEBOOK", e));
        }
    }

    /**
     * Create date info from order
     */
    private ProcessingDateInfo createDateInfo(FacebookOrderDto order) {
        if (order.getCreatedAt() == null) {
            return null;
        }

        try {
            LocalDateTime createdAt = parseDateTime(order.getCreatedAt());
            if (createdAt == null) {
                return null;
            }

            return ProcessingDateInfo.builder()
                    .orderId(order.getOrderId())
                    .dateKey(generateDateKey(createdAt))
                    .fullDate(createdAt)
                    .dayOfWeek(createdAt.getDayOfWeek().getValue())
                    .dayOfWeekName(createdAt.getDayOfWeek().name())
                    .dayOfMonth(createdAt.getDayOfMonth())
                    .dayOfYear(createdAt.getDayOfYear())
                    .weekOfYear(createdAt.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR))
                    .monthOfYear(createdAt.getMonthValue())
                    .monthName(createdAt.getMonth().name())
                    .quarterOfYear((createdAt.getMonthValue() - 1) / 3 + 1)
                    .quarterName("Q" + ((createdAt.getMonthValue() - 1) / 3 + 1))
                    .year(createdAt.getYear())
                    .isWeekend(createdAt.getDayOfWeek().getValue() >= 6)
                    .isBusinessDay(createdAt.getDayOfWeek().getValue() <= 5)
                    .fiscalYear(createdAt.getYear())
                    .fiscalQuarter((createdAt.getMonthValue() - 1) / 3 + 1)
                    .build();

        } catch (Exception e) {
            log.warn("Failed to create date info for order {}: {}", order.getOrderId(), e.getMessage());
            return null;
        }
    }

    private LocalDateTime parseDateTime(String dateTime) {
        try {
            return LocalDateTime.parse(dateTime.replace("Z", ""));
        } catch (Exception e) {
            return null;
        }
    }

    private Long generateDateKey(LocalDateTime dateTime) {
        return Long.valueOf(dateTime.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
    }
}