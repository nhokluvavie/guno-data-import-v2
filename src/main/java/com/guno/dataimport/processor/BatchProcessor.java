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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * BatchProcessor - Multi-platform data processing with comprehensive error handling
 * PATTERN: Temp table strategy with per-platform transaction isolation
 * PHASE 1.4: Enhanced error handling - no single order failure crashes entire batch
 * FIXED: Removed OrderStatusDetail (table doesn't exist in schema)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final FacebookMapper facebookMapper;
    private final TikTokMapper tikTokMapper;
    private final ShopeeMapper shopeeMapper;

    private final CustomerRepository customerRepository;
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    private final GeographyRepository geographyRepository;
    private final PaymentRepository paymentRepository;
    private final ShippingRepository shippingRepository;
    private final ProcessingDateRepository processingDateRepository;
    private final OrderStatusRepository orderStatusRepository;

    /**
     * MAIN ENTRY POINT: Process multi-platform data with comprehensive error handling
     */
    public ProcessingResult processCollectedData(CollectedData collectedData) {
        // Validation
        if (collectedData == null) {
            log.warn("Received null CollectedData");
            return ProcessingResult.builder()
                    .failedCount(0)
                    .build();
        }

        if (collectedData.getTotalOrders() == 0) {
            log.info("No orders to process");
            return ProcessingResult.builder().build();
        }

        long startTime = System.currentTimeMillis();
        ProcessingResult globalResult = ProcessingResult.builder().build();

        try {
            log.info("🚀 Starting multi-platform processing - Facebook: {}, TikTok: {}, Shopee: {}, Total: {}",
                    collectedData.getFacebookOrders().size(),
                    collectedData.getTikTokOrders().size(),
                    collectedData.getShopeeOrders().size(),
                    collectedData.getTotalOrders());

            // Process Facebook (independent transaction)
            if (!collectedData.getFacebookOrders().isEmpty()) {
                try {
                    log.info("📘 Processing {} Facebook orders...", collectedData.getFacebookOrders().size());
                    ProcessingResult fbResult = processFacebookWithTransaction(collectedData.getFacebookOrders());
                    globalResult.merge(fbResult);
                    log.info("✅ Facebook completed - Success: {}, Failed: {}",
                            fbResult.getSuccessCount(), fbResult.getFailedCount());
                } catch (Exception e) {
                    log.error("❌ Facebook batch failed: {}", e.getMessage(), e);
                    globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getFacebookOrders().size());
                    globalResult.getErrors().add(ErrorReport.of("FACEBOOK_BATCH", "ALL", "FACEBOOK", e));
                }
            }

            // Process TikTok (independent transaction)
            if (!collectedData.getTikTokOrders().isEmpty()) {
                try {
                    log.info("🎵 Processing {} TikTok orders...", collectedData.getTikTokOrders().size());
                    ProcessingResult ttResult = processTikTokWithTransaction(collectedData.getTikTokOrders());
                    globalResult.merge(ttResult);
                    log.info("✅ TikTok completed - Success: {}, Failed: {}",
                            ttResult.getSuccessCount(), ttResult.getFailedCount());
                } catch (Exception e) {
                    log.error("❌ TikTok batch failed: {}", e.getMessage(), e);
                    globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getTikTokOrders().size());
                    globalResult.getErrors().add(ErrorReport.of("TIKTOK_BATCH", "ALL", "TIKTOK", e));
                }
            }

            // Process Shopee (independent transaction)
            if (!collectedData.getShopeeOrders().isEmpty()) {
                try {
                    log.info("🛒 Processing {} Shopee orders...", collectedData.getShopeeOrders().size());
                    ProcessingResult spResult = processShopeeWithTransaction(collectedData.getShopeeOrders());
                    globalResult.merge(spResult);
                    log.info("✅ Shopee completed - Success: {}, Failed: {}",
                            spResult.getSuccessCount(), spResult.getFailedCount());
                } catch (Exception e) {
                    log.error("❌ Shopee batch failed: {}", e.getMessage(), e);
                    globalResult.setFailedCount(globalResult.getFailedCount() + collectedData.getShopeeOrders().size());
                    globalResult.getErrors().add(ErrorReport.of("SHOPEE_BATCH", "ALL", "SHOPEE", e));
                }
            }

            globalResult.setProcessingTimeMs(System.currentTimeMillis() - startTime);

            log.info("🎯 Multi-platform processing completed - Success: {}, Failed: {}, Duration: {}ms, Success Rate: {:.1f}%",
                    globalResult.getSuccessCount(),
                    globalResult.getFailedCount(),
                    globalResult.getProcessingTimeMs(),
                    globalResult.getSuccessRate());

            return globalResult;

        } catch (Exception e) {
            log.error("💥 Critical error in batch processing: {}", e.getMessage(), e);
            globalResult.setProcessingTimeMs(System.currentTimeMillis() - startTime);
            globalResult.setFailedCount(collectedData.getTotalOrders());
            globalResult.getErrors().add(ErrorReport.of("CRITICAL", "BATCH", "SYSTEM", e));
            return globalResult;
        }
    }

    // ================================
    // TRANSACTION WRAPPERS
    // ================================

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processFacebookWithTransaction(List<Object> facebookOrders) {
        return processFacebookOrders(facebookOrders);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processTikTokWithTransaction(List<Object> tikTokOrders) {
        return processTikTokOrders(tikTokOrders);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public ProcessingResult processShopeeWithTransaction(List<Object> shopeeOrders) {
        return processShopeeOrders(shopeeOrders);
    }

    // ================================
    // FACEBOOK PROCESSING
    // ================================

    public ProcessingResult processFacebookOrders(List<Object> facebookOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (facebookOrderObjects == null || facebookOrderObjects.isEmpty()) {
            return result;
        }

        List<FacebookOrderDto> facebookOrders = facebookOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (facebookOrders.isEmpty()) {
            log.warn("No valid FacebookOrderDto objects in input");
            return result;
        }

        try {
            log.info("📘 Mapping {} Facebook orders...", facebookOrders.size());

            // Map all entities with individual error handling
            List<Customer> customers = mapFacebookCustomers(facebookOrders, result);
            List<Order> orders = mapFacebookOrders(facebookOrders, result);
            List<OrderItem> orderItems = mapFacebookOrderItems(facebookOrders, result);
            List<Product> products = mapFacebookProducts(facebookOrders, result);
            List<GeographyInfo> geography = mapFacebookGeography(facebookOrders, result);
            List<PaymentInfo> payments = mapFacebookPayments(facebookOrders, result);
            List<ShippingInfo> shipping = mapFacebookShipping(facebookOrders, result);
            List<ProcessingDateInfo> dates = mapFacebookDates(facebookOrders, result);
            List<OrderStatus> orderStatuses = mapFacebookOrderStatuses(facebookOrders, result);

            log.info("📘 Upserting Facebook entities via temp tables...");

            // FIXED: Correct insert sequence respecting FK constraints

            // Step 1: Master data (no dependencies)
            customerRepository.bulkUpsert(customers);
            productRepository.bulkUpsert(products);

            // Step 2: Fact table (ORDER must be inserted BEFORE dimension tables that reference it)
            orderRepository.bulkUpsert(orders);

            // Step 3: Dimension tables (have FK to order_id)
            geographyRepository.bulkUpsert(geography);
            paymentRepository.bulkUpsert(payments);
            shippingRepository.bulkUpsert(shipping);
            processingDateRepository.bulkUpsert(dates);

            // Step 4: Detail tables (have FK to both order_id and other tables)
            orderItemRepository.bulkUpsert(orderItems);
            orderStatusRepository.bulkUpsert(orderStatuses);

            result.setSuccessCount(facebookOrders.size() - result.getFailedCount());
            log.info("✅ Facebook processing completed - {} successful", result.getSuccessCount());

        } catch (Exception e) {
            log.error("❌ Facebook processing error: {}", e.getMessage(), e);
            result.setFailedCount(facebookOrders.size());
            result.getErrors().add(ErrorReport.of("FACEBOOK_PROCESSING", "BATCH", "FACEBOOK", e));
        }

        return result;
    }

    // ================================
    // TIKTOK PROCESSING
    // ================================

    public ProcessingResult processTikTokOrders(List<Object> tikTokOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (tikTokOrderObjects == null || tikTokOrderObjects.isEmpty()) {
            return result;
        }

        List<FacebookOrderDto> tikTokOrders = tikTokOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (tikTokOrders.isEmpty()) {
            log.warn("No valid TikTok orders in input");
            return result;
        }

        try {
            log.info("🎵 Mapping {} TikTok orders...", tikTokOrders.size());

            List<Customer> customers = mapTikTokCustomers(tikTokOrders, result);
            List<Order> orders = mapTikTokOrders(tikTokOrders, result);
            List<OrderItem> orderItems = mapTikTokOrderItems(tikTokOrders, result);
            List<Product> products = mapTikTokProducts(tikTokOrders, result);
            List<GeographyInfo> geography = mapTikTokGeography(tikTokOrders, result);
            List<PaymentInfo> payments = mapTikTokPayments(tikTokOrders, result);
            List<ShippingInfo> shipping = mapTikTokShipping(tikTokOrders, result);
            List<ProcessingDateInfo> dates = mapTikTokDates(tikTokOrders, result);
            List<OrderStatus> orderStatuses = mapTikTokOrderStatuses(tikTokOrders, result);

            log.info("🎵 Upserting TikTok entities via temp tables...");

            // FIXED: Correct insert sequence respecting FK constraints

            // Step 1: Master data (no dependencies)
            customerRepository.bulkUpsert(customers);
            productRepository.bulkUpsert(products);

            // Step 2: Fact table (ORDER must be inserted BEFORE dimension tables that reference it)
            orderRepository.bulkUpsert(orders);

            // Step 3: Dimension tables (have FK to order_id)
            geographyRepository.bulkUpsert(geography);
            paymentRepository.bulkUpsert(payments);
            shippingRepository.bulkUpsert(shipping);
            processingDateRepository.bulkUpsert(dates);

            // Step 4: Detail tables (have FK to both order_id and other tables)
            orderItemRepository.bulkUpsert(orderItems);
            orderStatusRepository.bulkUpsert(orderStatuses);

            result.setSuccessCount(tikTokOrders.size() - result.getFailedCount());
            log.info("✅ TikTok processing completed - {} successful", result.getSuccessCount());

        } catch (Exception e) {
            log.error("❌ TikTok processing error: {}", e.getMessage(), e);
            result.setFailedCount(tikTokOrders.size());
            result.getErrors().add(ErrorReport.of("TIKTOK_PROCESSING", "BATCH", "TIKTOK", e));
        }

        return result;
    }

    // ================================
    // SHOPEE PROCESSING
    // ================================

    public ProcessingResult processShopeeOrders(List<Object> shopeeOrderObjects) {
        ProcessingResult result = ProcessingResult.builder().build();

        if (shopeeOrderObjects == null || shopeeOrderObjects.isEmpty()) {
            return result;
        }

        List<FacebookOrderDto> shopeeOrders = shopeeOrderObjects.stream()
                .filter(obj -> obj instanceof FacebookOrderDto)
                .map(obj -> (FacebookOrderDto) obj)
                .toList();

        if (shopeeOrders.isEmpty()) {
            log.warn("No valid Shopee orders in input");
            return result;
        }

        try {
            log.info("🛒 Mapping {} Shopee orders...", shopeeOrders.size());

            List<Customer> customers = mapShopeeCustomers(shopeeOrders, result);
            List<Order> orders = mapShopeeOrders(shopeeOrders, result);
            List<OrderItem> orderItems = mapShopeeOrderItems(shopeeOrders, result);
            List<Product> products = mapShopeeProducts(shopeeOrders, result);
            List<GeographyInfo> geography = mapShopeeGeography(shopeeOrders, result);
            List<PaymentInfo> payments = mapShopeePayments(shopeeOrders, result);
            List<ShippingInfo> shipping = mapShopeeShipping(shopeeOrders, result);
            List<ProcessingDateInfo> dates = mapShopeeDates(shopeeOrders, result);
            List<OrderStatus> orderStatuses = mapShopeeOrderStatuses(shopeeOrders, result);

            log.info("🛒 Upserting Shopee entities via temp tables...");

            // FIXED: Correct insert sequence respecting FK constraints

            // Step 1: Master data (no dependencies)
            customerRepository.bulkUpsert(customers);
            productRepository.bulkUpsert(products);

            // Step 2: Fact table (ORDER must be inserted BEFORE dimension tables that reference it)
            orderRepository.bulkUpsert(orders);

            // Step 3: Dimension tables (have FK to order_id)
            geographyRepository.bulkUpsert(geography);
            paymentRepository.bulkUpsert(payments);
            shippingRepository.bulkUpsert(shipping);
            processingDateRepository.bulkUpsert(dates);

            // Step 4: Detail tables (have FK to both order_id and other tables)
            orderItemRepository.bulkUpsert(orderItems);
            orderStatusRepository.bulkUpsert(orderStatuses);

            result.setSuccessCount(shopeeOrders.size() - result.getFailedCount());
            log.info("✅ Shopee processing completed - {} successful", result.getSuccessCount());

        } catch (Exception e) {
            log.error("❌ Shopee processing error: {}", e.getMessage(), e);
            result.setFailedCount(shopeeOrders.size());
            result.getErrors().add(ErrorReport.of("SHOPEE_PROCESSING", "BATCH", "SHOPEE", e));
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
                        result.getErrors().add(ErrorReport.of("FB_CUSTOMER", order.getOrderId(), "FACEBOOK", e));
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
                        result.getErrors().add(ErrorReport.of("FB_ORDER", order.getOrderId(), "FACEBOOK", e));
                        result.setFailedCount(result.getFailedCount() + 1);
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
                result.getErrors().add(ErrorReport.of("FB_ORDER_ITEMS", order.getOrderId(), "FACEBOOK", e));
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
                result.getErrors().add(ErrorReport.of("FB_PRODUCTS", order.getOrderId(), "FACEBOOK", e));
            }
        }
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
                        result.getErrors().add(ErrorReport.of("FB_GEOGRAPHY", order.getOrderId(), "FACEBOOK", e));
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
                        result.getErrors().add(ErrorReport.of("FB_PAYMENT", order.getOrderId(), "FACEBOOK", e));
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
                        result.getErrors().add(ErrorReport.of("FB_SHIPPING", order.getOrderId(), "FACEBOOK", e));
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
                        result.getErrors().add(ErrorReport.of("FB_DATE", order.getOrderId(), "FACEBOOK", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }


    private List<OrderStatus> mapFacebookOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(facebookMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("FB_ORDER_STATUS", order.getOrderId(), "FACEBOOK", e));
            }
        }
        return allOrderStatuses;
    }

    // ================================
    // TIKTOK MAPPING METHODS
    // ================================

    private List<Customer> mapTikTokCustomers(List<FacebookOrderDto> orders, ProcessingResult result) {
        return orders.stream()
                .map(order -> {
                    try {
                        return tikTokMapper.mapToCustomer(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("TT_CUSTOMER", order.getOrderId(), "TIKTOK", e));
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
                        result.getErrors().add(ErrorReport.of("TT_ORDER", order.getOrderId(), "TIKTOK", e));
                        result.setFailedCount(result.getFailedCount() + 1);
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
                result.getErrors().add(ErrorReport.of("TT_ORDER_ITEMS", order.getOrderId(), "TIKTOK", e));
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
                result.getErrors().add(ErrorReport.of("TT_PRODUCTS", order.getOrderId(), "TIKTOK", e));
            }
        }
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
                        result.getErrors().add(ErrorReport.of("TT_GEOGRAPHY", order.getOrderId(), "TIKTOK", e));
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
                        result.getErrors().add(ErrorReport.of("TT_PAYMENT", order.getOrderId(), "TIKTOK", e));
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
                        result.getErrors().add(ErrorReport.of("TT_SHIPPING", order.getOrderId(), "TIKTOK", e));
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
                        return tikTokMapper.mapToProcessingDateInfo(order);
                    } catch (Exception e) {
                        result.getErrors().add(ErrorReport.of("TT_DATE", order.getOrderId(), "TIKTOK", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    private List<OrderStatus> mapTikTokOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(tikTokMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("TT_ORDER_STATUS", order.getOrderId(), "TIKTOK", e));
            }
        }
        return allOrderStatuses;
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
                        result.getErrors().add(ErrorReport.of("SP_CUSTOMER", order.getOrderId(), "SHOPEE", e));
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
                        result.getErrors().add(ErrorReport.of("SP_ORDER", order.getOrderId(), "SHOPEE", e));
                        result.setFailedCount(result.getFailedCount() + 1);
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
                result.getErrors().add(ErrorReport.of("SP_ORDER_ITEMS", order.getOrderId(), "SHOPEE", e));
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
                result.getErrors().add(ErrorReport.of("SP_PRODUCTS", order.getOrderId(), "SHOPEE", e));
            }
        }
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
                        result.getErrors().add(ErrorReport.of("SP_GEOGRAPHY", order.getOrderId(), "SHOPEE", e));
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
                        result.getErrors().add(ErrorReport.of("SP_PAYMENT", order.getOrderId(), "SHOPEE", e));
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
                        result.getErrors().add(ErrorReport.of("SP_SHIPPING", order.getOrderId(), "SHOPEE", e));
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
                        result.getErrors().add(ErrorReport.of("SP_DATE", order.getOrderId(), "SHOPEE", e));
                        return null;
                    }
                })
                .filter(date -> date != null)
                .toList();
    }

    private List<OrderStatus> mapShopeeOrderStatuses(List<FacebookOrderDto> orders, ProcessingResult result) {
        List<OrderStatus> allOrderStatuses = new ArrayList<>();
        for (FacebookOrderDto order : orders) {
            try {
                allOrderStatuses.addAll(shopeeMapper.mapToOrderStatus(order));
            } catch (Exception e) {
                result.getErrors().add(ErrorReport.of("SP_ORDER_STATUS", order.getOrderId(), "SHOPEE", e));
            }
        }
        return allOrderStatuses;
    }
}