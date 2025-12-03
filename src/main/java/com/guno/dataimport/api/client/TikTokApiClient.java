package com.guno.dataimport.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.guno.dataimport.dto.platform.tiktok.TikTokApiResponse;
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
 * TikTok API Client - Updated to use TikTokApiResponse
 * Pattern: Independent from Facebook, clean separation
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TikTokApiClient {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Value("${api.tiktok.base-url}")
    private String baseUrl;

    @Value("${api.tiktok.headers.Authorization:}")
    private String authToken;

    @Value("${api.tiktok.headers.X-API-Key:}")
    private String apiKey;

    @Value("${api.tiktok.page-size:1500}")
    private int defaultPageSize;

    @Value("${api.tiktok.max-retries:5}")
    private int maxRetries;

    @Value("${api.tiktok.default-date:}")
    private String defaultDate;

    @Value("${api.tiktok.source:tiktok}")
    private String defaultSource;

    @Value("${api.tiktok.filter-date:update}")
    private String filterDate;

    /**
     * Fetch orders with default parameters
     */
    public TikTokApiResponse fetchOrders() {
        String date = defaultDate.isEmpty() ?
                LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) : defaultDate;
        return fetchOrders(date, 1, defaultPageSize);
    }

    /**
     * Fetch orders with specific date
     */
    public TikTokApiResponse fetchOrders(String date) {
        return fetchOrders(date, 1, defaultPageSize);
    }

    /**
     * Fetch orders with pagination and date
     */
    public TikTokApiResponse fetchOrders(String date, int page, int pageSize) {
        return fetchOrders(date, page, pageSize, defaultSource);
    }

    /**
     * Fetch orders with full parameters
     */
    public TikTokApiResponse fetchOrders(String date, int page, int pageSize, String source) {
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
    private TikTokApiResponse callApiWithRetry(String url, Map<String, Object> params) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Calling TikTok API - attempt {}/{}, params: {}", attempt, maxRetries, params);

                ResponseEntity<String> response = restTemplate.exchange(
                        buildUrlWithParams(url, params),
                        HttpMethod.GET,
                        createHttpEntity(),
                        String.class
                );

                TikTokApiResponse apiResponse = objectMapper.readValue(response.getBody(), TikTokApiResponse.class);

                if (apiResponse != null) {
                    log.info("Successfully fetched {} TikTok orders for date: {}",
                            apiResponse.getOrderCount(), params.get("date"));
                    return apiResponse;
                }

                log.warn("TikTok API returned null response body");

            } catch (Exception e) {
                lastException = e;
                log.warn("TikTok API call failed - attempt {}/{}: {}",
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

        log.error("TikTok API call failed after {} attempts", maxRetries, lastException);
        return createEmptyResponse();
    }

    /**
     * Check if TikTok API is available
     */
    public boolean isApiAvailable() {
        try {
            String testDate = LocalDate.now().minusDays(1)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            log.debug("TikTok API availability check - Date: {}, PageSize: {}", testDate, defaultPageSize);
            TikTokApiResponse response = fetchOrders(testDate, 1, defaultPageSize);

            boolean available = response != null
                    && response.getData() != null
                    && response.getOrderCount() >= 0;

            log.debug("TikTok API availability result: {} (orders: {}, status: {}, code: {})",
                    available, response != null ? response.getOrderCount() : "null",
                    response != null ? response.getStatus() : "null",
                    response != null ? response.getCode() : "null");

            return available;

        } catch (Exception e) {
            log.warn("TikTok API availability check failed: {}", e.getMessage());
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

        return urlBuilder.substring(0, urlBuilder.length() - 1);
    }

    private TikTokApiResponse createEmptyResponse() {
        return TikTokApiResponse.builder()
                .status(500)
                .message("API call failed")
                .code(500)
                .data(TikTokApiResponse.TikTokDataWrapper.builder()
                        .count(0)
                        .page(1)
                        .build())
                .build();
    }
}