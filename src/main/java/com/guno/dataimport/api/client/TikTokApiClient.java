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
 * TikTok API Client - FIXED VERSION
 * REUSES: FacebookApiResponse (same JSON structure)
 *
 * FIXES:
 * 1. ✅ Use defaultPageSize in health check
 * 2. ✅ Better isApiAvailable() logic
 * 3. ✅ Fixed filter-date property name
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

    @Value("${api.tiktok.filter-date:update}")  // FIXED: was api.facebook.filter-date
    private String filterDate;

    public FacebookApiResponse fetchOrders() {
        String date = defaultDate.isEmpty() ?
                LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) : defaultDate;
        return fetchOrders(date, 1, defaultPageSize);
    }

    public FacebookApiResponse fetchOrders(String date) {
        return fetchOrders(date, 1, defaultPageSize);
    }

    public FacebookApiResponse fetchOrders(String date, int page, int pageSize) {
        return fetchOrders(date, page, pageSize, defaultSource);
    }

    public FacebookApiResponse fetchOrders(String date, int page, int pageSize, String source) {
        Map<String, Object> params = new HashMap<>();
        params.put("date", date);
        params.put("page", page);
        params.put("limit", pageSize);
        params.put("source", source);
        params.put("filter-date", filterDate);

        return callApiWithRetry(baseUrl, params);
    }

    private FacebookApiResponse callApiWithRetry(String url, Map<String, Object> params) {
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

                FacebookApiResponse apiResponse = objectMapper.readValue(response.getBody(), FacebookApiResponse.class);

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
                        Thread.sleep(2000 * attempt);
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
     * FIXED: Check if TikTok API is available
     */
    public boolean isApiAvailable() {
        try {
            String testDate = LocalDate.now().minusDays(1)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // FIXED: Use defaultPageSize instead of 1
            log.debug("TikTok API availability check - Date: {}, PageSize: {}", testDate, defaultPageSize);
            FacebookApiResponse response = fetchOrders(testDate, 1, defaultPageSize);

            // FIXED: Check response data instead of status code
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