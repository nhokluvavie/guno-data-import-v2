package com.guno.dataimport.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Facebook API Client - Calls actual Facebook API to fetch order data
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FacebookApiClient {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper; // Use injected ObjectMapper

    @Value("${api.facebook.base-url}")
    private String baseUrl;

    @Value("${api.facebook.headers.Authorization:}")
    private String authToken;

    @Value("${api.facebook.headers.X-API-Key:}")
    private String apiKey;

    @Value("${api.facebook.page-size:1000}")
    private int defaultPageSize;

    @Value("${api.facebook.max-retries:5}")
    private int maxRetries;

    @Value("${api.facebook.default-date:}")
    private String defaultDate;

    @Value("${api.facebook.source:facebook}")
    private String defaultSource;

    @Value("${api.facebook.filter-date:insert}")
    private String filterDate;

    /**
     * Fetch Facebook orders from API with default date (today)
     */
    public FacebookApiResponse fetchOrders() {
        String date = defaultDate.isEmpty() ? LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) : defaultDate;
        return fetchOrders(date, 1, defaultPageSize);
    }

    /**
     * Fetch orders with specific date
     */
    public FacebookApiResponse fetchOrders(String date) {
        return fetchOrders(date, 1, defaultPageSize);
    }

    /**
     * Fetch orders with pagination and date
     */
    public FacebookApiResponse fetchOrders(String date, int page, int pageSize) {
        return fetchOrders(date, page, pageSize, defaultSource);
    }

    /**
     * Fetch orders with full parameters
     */
    public FacebookApiResponse fetchOrders(String date, int page, int pageSize, String source) {
        Map<String, Object> params = new HashMap<>();
        params.put("date", date);
        params.put("page", page);
        params.put("limit", pageSize);
        params.put("source", source);
        params.put("filter-date", filterDate);

        return callApiWithRetry(baseUrl, params);
    }

    /**
     * Call API with retry mechanism
     */
    private FacebookApiResponse callApiWithRetry(String url, Map<String, Object> params) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Calling Facebook API - attempt {}/{}, params: {}", attempt, maxRetries, params);

                ResponseEntity<String> response = restTemplate.exchange(
                        buildUrlWithParams(url, params),
                        HttpMethod.GET,
                        createHttpEntity(),
                        String.class
                );

                // Debug raw response
                log.debug("Raw API Response: {}", response.getBody());

                // Parse manually to FacebookApiResponse
                FacebookApiResponse apiResponse = objectMapper.readValue(response.getBody(), FacebookApiResponse.class);

                if (apiResponse != null) {
                    log.info("Successfully fetched {} Facebook orders for date: {}",
                            apiResponse.getOrderCount(), params.get("date"));
                    return apiResponse;
                }

                log.warn("Facebook API returned null response body");

            } catch (Exception e) {
                lastException = e;
                log.warn("Facebook API call failed - attempt {}/{}: {}",
                        attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(2000 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.error("Facebook API call failed after {} attempts", maxRetries, lastException);
        return createEmptyResponse();
    }

    /**
     * Check if Facebook API is available
     */
    public boolean isApiAvailable() {
        try {
            // Use a simple API call to check availability
            String testDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            FacebookApiResponse response = fetchOrders(testDate, 1, 1);
            return response.getStatus() != null && response.getStatus() == 200;
        } catch (Exception e) {
            log.warn("Facebook API availability check failed: {}", e.getMessage());
            return false;
        }
    }

    private HttpEntity<Void> createHttpEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        if (authToken != null && !authToken.isEmpty() && apiKey != null && !apiKey.isEmpty()) {
            headers.set(authToken, apiKey);
        }

        return new HttpEntity<>(headers);
    }

    private String buildUrlWithParams(String url, Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return url;
        }

        StringBuilder urlBuilder = new StringBuilder(url).append("?");
        params.forEach((key, value) ->
                urlBuilder.append(key).append("=").append(value).append("&"));

        return urlBuilder.substring(0, urlBuilder.length() - 1); // Remove last &
    }

    private FacebookApiResponse createEmptyResponse() {
        return FacebookApiResponse.builder()
                .status(500)
                .message("API call failed")
                .code(500)
                .data(FacebookApiResponse.FacebookDataWrapper.builder()
                        .count(0)
                        .page(1)
                        .build())
                .build();
    }
}