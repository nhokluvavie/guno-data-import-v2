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
 * TikTok API Client - Calls TikTok API to fetch order data
 * REUSES: FacebookApiResponse (same JSON structure)
 * PATTERN: 95% identical to FacebookApiClient
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

    @Value("${api.tiktok.page-size:1000}")
    private int defaultPageSize;

    @Value("${api.tiktok.max-retries:5}")
    private int maxRetries;

    @Value("${api.tiktok.default-date:}")
    private String defaultDate;

    @Value("${api.tiktok.source:tiktok}")
    private String defaultSource;

    /**
     * Fetch TikTok orders from API with default date (today)
     */
    public FacebookApiResponse fetchOrders() {
        String date = defaultDate.isEmpty() ?
                LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) : defaultDate;
        return fetchOrders(date, 1, defaultPageSize);
    }

    /**
     * Fetch TikTok orders with specific date and pagination
     */
    public FacebookApiResponse fetchOrders(String date, int page, int pageSize) {
        return fetchOrders(date, page, pageSize, defaultSource);
    }

    /**
     * Fetch TikTok orders with full parameters
     */
    public FacebookApiResponse fetchOrders(String date, int page, int pageSize, String source) {
        log.debug("Fetching TikTok orders - Date: {}, Page: {}, Size: {}, Source: {}",
                date, page, pageSize, source);

        Map<String, Object> params = new HashMap<>();
        params.put("date", date);
        params.put("page", page);
        params.put("limit", pageSize);
        if (source != null && !source.isEmpty()) {
            params.put("source", source);
        }

        String url = buildUrlWithParams(baseUrl, params);
        HttpEntity<Void> entity = createHttpEntity();

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("TikTok API call attempt {}/{} - URL: {}", attempt, maxRetries, url);

                ResponseEntity<FacebookApiResponse> response = restTemplate.exchange(
                        url, HttpMethod.GET, entity, FacebookApiResponse.class);

                FacebookApiResponse result = response.getBody();
                if (result != null) {
                    log.info("TikTok API success - Status: {}, Orders: {}",
                            result.getStatus(), result.getOrderCount());
                    return result;
                }

            } catch (Exception e) {
                lastException = e;
                log.warn("TikTok API attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000 * attempt); // Progressive backoff
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
            String testDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            FacebookApiResponse response = fetchOrders(testDate, 1, 1);
            return response.getStatus() != null && response.getStatus() == 200;
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
                .message("TikTok API call failed")
                .code(500)
                .data(FacebookApiResponse.FacebookDataWrapper.builder()
                        .count(0)
                        .page(1)
                        .build())
                .build();
    }
}