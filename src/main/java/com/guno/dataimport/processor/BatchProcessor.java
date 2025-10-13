package com.guno.dataimport.processor;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.internal.ErrorReport;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.entity.*;
import com.guno.dataimport.mapper.FacebookMapper;
import com.guno.dataimport.mapper.ShopeeMapper;
import com.guno.dataimport.mapper.TikTokMapper;
import com.guno.dataimport.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * BatchProcessor - ENHANCED: Multi-platform support with TikTok, Shopee integration
 * PATTERN: Pure temp table approach, NO DELETE operations
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final FacebookMapper facebookMapper;
    private final TikTokMapper tikTokMapper;  // NEW: TikTok mapper
    private final ShopeeMapper shopeeMapper;  // NEW: Shopee mapper

    private final CustomerRepository customerRepository;
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    private final GeographyRepository geographyRepository;
    private final PaymentRepository paymentRepository;
    private final ShippingRepository shippingRepository;
    private final ProcessingDateRepository processingDateRepository;
    private final OrderStatusRepository orderStatusRepository;
    private final OrderStatusDetailRepository orderStatusDetailRepository;
    private final StatusRepository statusRepository;

    /**
     * MAIN ENTRY POINT: Process multi-platform data with temp table strategy
     */
    public ProcessingResult processCollectedData(CollectedData collectedData) {
        if (collectedData == null || collectedData.getTotalOrders() == 0) {
            return ProcessingResult.builder().build();
        }

        long startTime = System.currentTimeMillis();
        ProcessingResult globalResult = ProcessingResult.builder().build();

        log.info("Processing multi-platform data - Facebook: {}, TikTok: {}, Shopee: {}, Total: {}",
                collectedData.getFacebookOrders().size(),
                collectedData.getTikTokOrders().size(),
                collectedData.getShopeeOrders().size(),
                collectedData.getTotalOrders());

        // Process Facebook in separate transaction
        if (!collectedData.getFacebookOrders().isEmpty()) {
            try {
                log.info("Processing {} Facebook orders in separate transaction...",
                        collectedData.getFacebookOrders().size());
                ProcessingResult fbResult = processFacebookWithTransaction(collectedData.getFacebookOrders());
                globalResult.merge(fbResult);
                log.info("✅ Facebook: {} success, {} failed",
                        fbResult.getSuccessCount(), fbResult.getFailedCount());
            } catch (Exception e) {
                log.error("❌ Facebook transaction failed: {}", e.getMessage(), e);
                globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getFacebookOrders().size());
                globalResult.getErrors().add(ErrorReport.of("FACEBOOK_TX", "BATCH", "FACEBOOK", e));
            }
        }

        // Process TikTok in separate transaction
        if (!collectedData.getTikTokOrders().isEmpty()) {
            try {
                log.info("Processing {} TikTok orders in separate transaction...",
                        collectedData.getTikTokOrders().size());
                ProcessingResult ttResult = processTikTokWithTransaction(collectedData.getTikTokOrders());
                globalResult.merge(ttResult);
                log.info("✅ TikTok: {} success, {} failed",
                        ttResult.getSuccessCount(), ttResult.getFailedCount());
            } catch (Exception e) {
                log.error("❌ TikTok transaction failed: {}", e.getMessage(), e);
                globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getTikTokOrders().size());
                globalResult.getErrors().add(ErrorReport.of("TIKTOK_TX", "BATCH", "TIKTOK", e));
            }
        }

        // Process Shopee in separate transaction
        if (!collectedData.getShopeeOrders().isEmpty()) {
            try {
                log.info("Processing {} Shopee orders in separate transaction...",
                        collectedData.getShopeeOrders().size());
                ProcessingResult spResult = processShopeeWithTransaction(collectedData.getShopeeOrders());
                globalResult.merge(spResult);
                log.info("✅ Shopee: {} success, {} failed",
                        spResult.getSuccessCount(), spResult.getFailedCount());
            } catch (Exception e) {
                log.error("❌ Shopee transaction failed: {}", e.getMessage(), e);
                globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getShopeeOrders().size());
                globalResult.getErrors().add(ErrorReport.of("SHOPEE_TX", "BATCH", "SHOPEE", e));
            }
        }

        globalResult.setProcessingTimeMs(System.currentTimeMillis() - startTime);

        log.info("Multi-platform processing completed - Total success: {}, Total failed: {}, Duration: {}ms",
                globalResult.getSuccessCount(), globalResult.getFailedCount(), globalResult.getProcessingTimeMs());

        return globalResult;
    }

    /**
     * Process Facebook orders in a NEW transaction
     * REQUIRES_NEW = independent transaction that commits/rolls back separately
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processFacebookWithTransaction(List<Object> facebookOrders) {
        log.debug("Starting Facebook transaction...");
        ProcessingResult result = processFacebookOrders(facebookOrders);
        log.debug("Facebook transaction completed successfully");
        return result;
    }

    /**
     * Process TikTok orders in a NEW transaction
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processTikTokWithTransaction(List<Object> tikTokOrders) {
        log.debug("Starting TikTok transaction...");
        ProcessingResult result = processTikTokOrders(tikTokOrders);
        log.debug("TikTok transaction completed successfully");
        return result;
    }

    /**
     * Process Shopee orders in a NEW transaction
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processShopeeWithTransaction(List<Object> shopeeOrders) {
        log.debug("Starting Shopee transaction...");
        ProcessingResult result = processShopeeOrders(shopeeOrders);
        log.debug("Shopee transaction completed successfully");
        return result;
    }

    /**
     * FACEBOOK: Process Facebook orders using FacebookMapper
     */
    public ProcessingResult processFacebookOrders(List<Object> facebookOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (facebookOrderObjects == null || facebookOrderObjects.isEmpty()) {
            return result;
        }

        // Convert objects to FacebookOrderDto
        List<FacebookOrderDto> facebookOrders = facebookOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (facebookOrders.isEmpty()) {
            log.warn("No valid FacebookOrderDto objects found in input");
            return result;
        }

        log.info("Processing {} Facebook orders with TEMP TABLE strategy", facebookOrders.size());

        try {
            // Map entities using FacebookMapper
            List<Customer> customers = mapFacebookCustomers(facebookOrders, result);
            List<Order> orders = mapFacebookOrders(facebookOrders, result);
            List<OrderItem> orderItems = mapFacebookOrderItems(facebookOrders, result);
            List<Product> products = mapFacebookProducts(facebookOrders, result);
            List<GeographyInfo> geography = mapFacebookGeography(facebookOrders, result);
            List<PaymentInfo> payments = mapFacebookPayments(facebookOrders, result);
            List<ShippingInfo> shipping = mapFacebookShipping(facebookOrders, result);
            List<ProcessingDateInfo> dates = mapFacebookDates(facebookOrders, result);
            List<Status> statuses = mapFacebookStatuses(facebookOrders, result);
            List<OrderStatus> orderStatuses = mapFacebookOrderStatuses(facebookOrders, result);
            List<OrderStatusDetail> orderStatusDetails = mapFacebookOrderStatusDetails(facebookOrders, result);

            // Bulk upsert all entities via temp tables
            log.debug("Upserting Facebook entities via temp tables...");

            // 1. Master data first
            customerRepository.bulkUpsert(customers);
            log.debug("✅ Facebook customers upserted via temp table");

            statusRepository.bulkUpsert(statuses);
            log.debug("✅ Facebook statuses upserted via temp table");

            productRepository.bulkUpsert(products);            // ✅ MOVED UP
            log.debug("✅ Facebook products upserted via temp table");

// 2. Dependent tables
            orderRepository.bulkUpsert(orders);
            log.debug("✅ Facebook orders upserted via temp table");

            geographyRepository.bulkUpsert(geography);
            log.debug("✅ Facebook geography upserted via temp table");

            paymentRepository.bulkUpsert(payments);
            log.debug("✅ Facebook payments upserted via temp table");

            shippingRepository.bulkUpsert(shipping);
            log.debug("✅ Facebook shipping upserted via temp table");

            processingDateRepository.bulkUpsert(dates);
            log.debug("✅ Facebook processing dates upserted via temp table");

// 3. Multi-dependency tables last
            orderItemRepository.bulkUpsert(orderItems);        // ✅ MOVED DOWN
            log.debug("✅ Facebook order items upserted via temp table");

            orderStatusRepository.bulkUpsert(orderStatuses);
            log.debug("✅ Facebook order statuses upserted via temp table");

            orderStatusDetailRepository.bulkUpsert(orderStatusDetails);
            log.debug("✅ Facebook order statuses detail upserted via temp table");

            result.setSuccessCount(facebookOrders.size() - result.getFailedCount());
            log.info("Successfully processed {} Facebook orders via TEMP TABLE strategy", result.getSuccessCount());

        } catch (Exception e) {
            log.error("Facebook TEMP TABLE processing error: {}", e.getMessage(), e);
            result.setFailedCount(facebookOrders.size());
            result.getErrors().add(ErrorReport.of("FACEBOOK_ORDERS", "BATCH", "FACEBOOK", e));
        }

        return result;
    }

    /**
     * TIKTOK: Process TikTok orders using TikTokMapper
     */
    public ProcessingResult processTikTokOrders(List<Object> tikTokOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (tikTokOrderObjects == null || tikTokOrderObjects.isEmpty()) {
            return result;
        }

        // Convert objects to FacebookOrderDto (TikTok reuses same structure!)
        List<FacebookOrderDto> tikTokOrders = tikTokOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (tikTokOrders.isEmpty()) {
            log.warn("No valid TikTok order objects found in input");
            return result;
        }

        log.info("Processing {} TikTok orders with TEMP TABLE strategy", tikTokOrders.size());

        try {
            // Map entities using TikTokMapper
            List<Customer> customers = mapTikTokCustomers(tikTokOrders, result);
            List<Order> orders = mapTikTokOrders(tikTokOrders, result);
            List<OrderItem> orderItems = mapTikTokOrderItems(tikTokOrders, result);
            List<Product> products = mapTikTokProducts(tikTokOrders, result);
            List<GeographyInfo> geography = mapTikTokGeography(tikTokOrders, result);
            List<PaymentInfo> payments = mapTikTokPayments(tikTokOrders, result);
            List<ShippingInfo> shipping = mapTikTokShipping(tikTokOrders, result);
            List<ProcessingDateInfo> dates = mapTikTokDates(tikTokOrders, result);
            List<Status> statuses = mapTikTokStatuses(tikTokOrders, result);
            List<OrderStatus> orderStatuses = mapTikTokOrderStatuses(tikTokOrders, result);
            List<OrderStatusDetail> orderStatusDetails = mapTikTokOrderStatusDetails(tikTokOrders, result);

            // FIXED: Bulk upsert in CORRECT ORDER to avoid foreign key violations
            log.debug("Upserting TikTok entities via temp tables...");

            // 1. Master data first (no dependencies)
            customerRepository.bulkUpsert(customers);
            log.debug("✅ TikTok customers upserted via temp table");

            statusRepository.bulkUpsert(statuses);
            log.debug("✅ TikTok statuses upserted via temp table");

            productRepository.bulkUpsert(products);            // ✅ MOVED UP - MUST BE BEFORE orderItems
            log.debug("✅ TikTok products upserted via temp table");

            // 2. Dependent tables (depend on customers, statuses, products)
            orderRepository.bulkUpsert(orders);               // ✅ MUST BE BEFORE orderItems
            log.debug("✅ TikTok orders upserted via temp table");

            geographyRepository.bulkUpsert(geography);
            log.debug("✅ TikTok geography upserted via temp table");

            paymentRepository.bulkUpsert(payments);
            log.debug("✅ TikTok payments upserted via temp table");

            shippingRepository.bulkUpsert(shipping);
            log.debug("✅ TikTok shipping upserted via temp table");

            processingDateRepository.bulkUpsert(dates);
            log.debug("✅ TikTok processing dates upserted via temp table");

            // 3. Multi-dependency tables last (depend on orders + products + statuses)
            orderItemRepository.bulkUpsert(orderItems);        // ✅ MOVED DOWN - AFTER products + orders
            log.debug("✅ TikTok order items upserted via temp table");

            orderStatusRepository.bulkUpsert(orderStatuses);   // ✅ AFTER orders + statuses
            log.debug("✅ TikTok order statuses upserted via temp table");

            orderStatusDetailRepository.bulkUpsert(orderStatusDetails); // ✅ AFTER orderStatuses
            log.debug("✅ TikTok order status details upserted via temp table");

            result.setSuccessCount(tikTokOrders.size() - result.getFailedCount());
            log.info("Successfully processed {} TikTok orders via TEMP TABLE strategy", result.getSuccessCount());

        } catch (Exception e) {
            log.error("TikTok TEMP TABLE processing error: {}", e.getMessage(), e);
            result.setFailedCount(tikTokOrders.size());
            result.getErrors().add(ErrorReport.of("TIKTOK_ORDERS", "BATCH", "TIKTOK", e));
        }

        return result;
    }

    /**
     * TIKTOK: Process TikTok orders using TikTokMapper
     */
    public ProcessingResult processShopeeOrders(List<Object> shopeeOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (shopeeOrderObjects == null || shopeeOrderObjects.isEmpty()) {
            return result;
        }

        // Convert objects to FacebookOrderDto (Shopee reuses same structure!)
        List<FacebookOrderDto> shopeeOrders = shopeeOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (shopeeOrders.isEmpty()) {
            log.warn("No valid Shopee order objects found in input");
            return result;
        }

        log.info("Processing {} Shopee orders with TEMP TABLE strategy", shopeeOrders.size());

        try {
            // Map entities using ShopeeMapper
            List<Customer> customers = mapShopeeCustomers(shopeeOrders, result);
            List<Order> orders = mapShopeeOrders(shopeeOrders, result);
            List<OrderItem> orderItems = mapShopeeOrderItems(shopeeOrders, result);
            List<Product> products = mapShopeeProducts(shopeeOrders, result);
            List<GeographyInfo> geography = mapShopeeGeography(shopeeOrders, result);
            List<PaymentInfo> payments = mapShopeePayments(shopeeOrders, result);
            List<ShippingInfo> shipping = mapShopeeShipping(shopeeOrders, result);
            List<ProcessingDateInfo> dates = mapShopeeDates(shopeeOrders, result);
            List<Status> statuses = mapShopeeStatuses(shopeeOrders, result);
            List<OrderStatus> orderStatuses = mapShopeeOrderStatuses(shopeeOrders, result);
            List<OrderStatusDetail> orderStatusDetails = mapShopeeOrderStatusDetails(shopeeOrders, result);

            // FIXED: Bulk upsert in CORRECT ORDER to avoid foreign key violations
            log.debug("Upserting Shopee entities via temp tables...");

            // 1. Master data first (no dependencies)
            customerRepository.bulkUpsert(customers);
            log.debug("✅ Shopee customers upserted via temp table");

            statusRepository.bulkUpsert(statuses);
            log.debug("✅ Shopee statuses upserted via temp table");

            productRepository.bulkUpsert(products);            // ✅ MOVED UP - MUST BE BEFORE orderItems
            log.debug("✅ Shopee products upserted via temp table");

            // 2. Dependent tables (depend on customers, statuses, products)
            orderRepository.bulkUpsert(orders);               // ✅ MUST BE BEFORE orderItems
            log.debug("✅ Shopee orders upserted via temp table");

            geographyRepository.bulkUpsert(geography);
            log.debug("✅ Shopee geography upserted via temp table");

            paymentRepository.bulkUpsert(payments);
            log.debug("✅ Shopee payments upserted via temp table");

            shippingRepository.bulkUpsert(shipping);
            log.debug("✅ Shopee shipping upserted via temp table");

            processingDateRepository.bulkUpsert(dates);
            log.debug("✅ Shopee processing dates upserted via temp table");

            // 3. Multi-dependency tables last (depend on orders + products + statuses)
            orderItemRepository.bulkUpsert(orderItems);        // ✅ MOVED DOWN - AFTER products + orders
            log.debug("✅ Shopee order items upserted via temp table");

            orderStatusRepository.bulkUpsert(orderStatuses);   // ✅ AFTER orders + statuses
            log.debug("✅ Shopee order statuses upserted via temp table");

            orderStatusDetailRepository.bulkUpsert(orderStatusDetails); // ✅ AFTER orderStatuses
            log.debug("✅ Shopee order status details upserted via temp table");

            result.setSuccessCount(shopeeOrders.size() - result.getFailedCount());
            log.info("Successfully processed {} Shopee orders via TEMP TABLE strategy", result.getSuccessCount());

        } catch (Exception e) {
            log.error("Shopee TEMP TABLE processing error: {}", e.getMessage(), e);
            result.setFailedCount(shopeeOrders.size());
            result.getErrors().add(ErrorReport.of("SHOPEE_ORDERS", "BATCH", "SHOPEE", e));
        }

        return result;
    }

    // ================================
    // FACEBOOK MAPPING METHODS
    // ================================

    private List<Customer> mapFacebookCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToCustomer(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_CUSTOMER", order.getOrderId(), "FACEBOOK", e));
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

    private List<Order> mapFacebookOrders(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToOrder(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_ORDER", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(order -> order != null)
                .toList();
    }

    private List<OrderItem> mapFacebookOrderItems(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderItem> allItems = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allItems.addAll(facebookMapper.mapToOrderItems(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FACEBOOK_ORDER_ITEMS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allItems;
    }

    private List<Product> mapFacebookProducts(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Product> allProducts = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allProducts.addAll(facebookMapper.mapToProducts(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FACEBOOK_PRODUCTS", order.getOrderId(), "FACEBOOK", e));
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

    private List<GeographyInfo> mapFacebookGeography(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToGeographyInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_GEOGRAPHY", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(geo -> geo != null)
                .toList();
    }

    private List<PaymentInfo> mapFacebookPayments(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToPaymentInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_PAYMENT", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(payment -> payment != null)
                .toList();
    }

    private List<ShippingInfo> mapFacebookShipping(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToShippingInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_SHIPPING", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(shipping -> shipping != null)
                .toList();
    }

    private List<ProcessingDateInfo> mapFacebookDates(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return facebookMapper.mapToProcessingDateInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("FACEBOOK_DATE", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    private List<Status> mapFacebookStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Status> allStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allStatuses.addAll(facebookMapper.mapToStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FACEBOOK_STATUS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allStatuses.stream().distinct().toList();
    }

    private List<OrderStatus> mapFacebookOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(facebookMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FACEBOOK_ORDER_STATUS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allOrderStatuses;
    }

    private List<OrderStatusDetail> mapFacebookOrderStatusDetails(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatusDetail> allDetails = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allDetails.addAll(facebookMapper.mapToOrderStatusDetail(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FACEBOOK_ORDER_STATUS_DETAIL", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allDetails;
    }

    // ================================
    // TIKTOK MAPPING METHODS
    // ================================

    private List<Customer> mapTikTokCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .filter(order -> order.getCustomer() != null && order.getCustomer().getId() != null)
                .map(order -> {
                    try {
                        return tikTokMapper.mapToCustomer(order);
                    } catch (Exception e) {
                        log.error("Failed mapping TikTok customer for order {}: {}",
                                order.getOrderId(), e.getMessage());
                        result.getErrors().add(ErrorReport.of("TIKTOK_CUSTOMER", order.getOrderId(), "TIKTOK", e));
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

    private List<Order> mapTikTokOrders(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return tikTokMapper.mapToOrder(order);
                    } catch (Exception e) {
                        log.error("Failed mapping TikTok order {}: {}", order.getOrderId(), e.getMessage());
                        result.getErrors().add(ErrorReport.of("TIKTOK_ORDER", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(order -> order != null)
                .toList();
    }

    private List<OrderItem> mapTikTokOrderItems(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderItem> allItems = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allItems.addAll(tikTokMapper.mapToOrderItems(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TIKTOK_ORDER_ITEMS", order.getOrderId(), "TIKTOK", e));
            }
        }
        return allItems;
    }

    private List<Product> mapTikTokProducts(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Product> allProducts = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allProducts.addAll(tikTokMapper.mapToProducts(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TIKTOK_PRODUCTS", order.getOrderId(), "TIKTOK", e));
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

    private List<GeographyInfo> mapTikTokGeography(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return tikTokMapper.mapToGeographyInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("TIKTOK_GEOGRAPHY", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(geo -> geo != null)
                .toList();
    }

    private List<PaymentInfo> mapTikTokPayments(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return tikTokMapper.mapToPaymentInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("TIKTOK_PAYMENT", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(payment -> payment != null)
                .toList();
    }

    private List<ShippingInfo> mapTikTokShipping(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return tikTokMapper.mapToShippingInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("TIKTOK_SHIPPING", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(shipping -> shipping != null)
                .toList();
    }

    private List<ProcessingDateInfo> mapTikTokDates(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        ProcessingDateInfo date = tikTokMapper.mapToProcessingDateInfo(order);
                        return date;
                    } catch (Exception e) {
                        log.error("  ❌ Failed mapping order {}: {}", order.getOrderId(), e.getMessage());
                        result.getErrors().add(ErrorReport.of("TIKTOK_DATE", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    private List<Status> mapTikTokStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Status> allStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allStatuses.addAll(tikTokMapper.mapToStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TIKTOK_STATUS", order.getOrderId(), "TIKTOK", e));
            }
        }
        return allStatuses.stream().distinct().toList();
    }

    private List<OrderStatus> mapTikTokOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(tikTokMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TIKTOK_ORDER_STATUS", order.getOrderId(), "TIKTOK", e));
            }
        }
        return allOrderStatuses;
    }

    private List<OrderStatusDetail> mapTikTokOrderStatusDetails(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatusDetail> allDetails = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allDetails.addAll(tikTokMapper.mapToOrderStatusDetail(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TIKTOK_ORDER_STATUS_DETAIL", order.getOrderId(), "TIKTOK", e));
            }
        }
        return allDetails;
    }

    // ================================
    // SHOPEE MAPPING METHODS
    // ================================

    private List<Customer> mapShopeeCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToCustomer(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_CUSTOMER", order.getOrderId(), "SHOPEE", e));
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

    private List<Order> mapShopeeOrders(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToOrder(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_ORDER", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(order -> order != null)
                .toList();
    }

    private List<OrderItem> mapShopeeOrderItems(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderItem> allItems = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allItems.addAll(shopeeMapper.mapToOrderItems(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SHOPEE_ORDER_ITEMS", order.getOrderId(), "SHOPEE", e));
            }
        }
        return allItems;
    }

    private List<Product> mapShopeeProducts(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Product> allProducts = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allProducts.addAll(shopeeMapper.mapToProducts(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SHOPEE_PRODUCTS", order.getOrderId(), "SHOPEE", e));
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

    private List<GeographyInfo> mapShopeeGeography(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToGeographyInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_GEOGRAPHY", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(geo -> geo != null)
                .toList();
    }

    private List<PaymentInfo> mapShopeePayments(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToPaymentInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_PAYMENT", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(payment -> payment != null)
                .toList();
    }

    private List<ShippingInfo> mapShopeeShipping(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToShippingInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_SHIPPING", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(shipping -> shipping != null)
                .toList();
    }

    private List<ProcessingDateInfo> mapShopeeDates(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return shopeeMapper.mapToProcessingDateInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("SHOPEE_DATE", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    private List<Status> mapShopeeStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<Status> allStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allStatuses.addAll(shopeeMapper.mapToStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SHOPEE_STATUS", order.getOrderId(), "SHOPEE", e));
            }
        }
        return allStatuses.stream().distinct().toList();
    }

    private List<OrderStatus> mapShopeeOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(shopeeMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SHOPEE_ORDER_STATUS", order.getOrderId(), "SHOPEE", e));
            }
        }
        return allOrderStatuses;
    }

    private List<OrderStatusDetail> mapShopeeOrderStatusDetails(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatusDetail> allDetails = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allDetails.addAll(shopeeMapper.mapToOrderStatusDetail(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SHOPEE_ORDER_STATUS_DETAIL", order.getOrderId(), "SHOPEE", e));
            }
        }
        return allDetails;
    }

    // ================================
    // UTILITY METHODS
    // ================================

    /**
     * Get processing statistics
     */
    public String getProcessingStats() {
        return String.format("BatchProcessor ready for multi-platform processing at %s",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    /**
     * Validate system readiness
     */
    public boolean isSystemReady() {
        try {
            return customerRepository != null &&
                    orderRepository != null &&
                    facebookMapper != null &&
                    tikTokMapper != null;
        } catch (Exception e) {
            log.warn("System readiness check failed: {}", e.getMessage());
            return false;
        }
    }
}