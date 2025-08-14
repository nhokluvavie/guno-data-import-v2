package com.guno.dataimport.api.client;

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

    @Value("${api.facebook.base-url}")
    private String baseUrl;

    @Value("${api.facebook.headers.Authorization:}")
    private String authToken;

    @Value("${api.facebook.headers.X-API-Key:}")
    private String apiKey;

    @Value("${api.facebook.timeout-seconds:600}")
    private int timeoutSeconds;

    @Value("${api.facebook.page-size:1000}")
    private int defaultPageSize;

    @Value("${api.facebook.max-retries:5}")
    private int maxRetries;

    /**
     * Fetch Facebook orders from API
     */
    public FacebookApiResponse fetchOrders() {
        return fetchOrders(1, defaultPageSize);
    }

    /**
     * Fetch orders with pagination
     */
    public FacebookApiResponse fetchOrders(int page, int pageSize) {
        String url = baseUrl + "/orders";

        Map<String, Object> params = new HashMap<>();
        params.put("page", page);
        params.put("limit", pageSize);

        return callApiWithRetry(url, params);
    }

    /**
     * Call API with retry mechanism
     */
    private FacebookApiResponse callApiWithRetry(String url, Map<String, Object> params) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Calling Facebook API - attempt {}/{}", attempt, maxRetries);

                ResponseEntity<FacebookApiResponse> response = restTemplate.exchange(
                        buildUrlWithParams(url, params),
                        HttpMethod.GET,
                        createHttpEntity(),
                        FacebookApiResponse.class
                );

                if (response.getBody() != null && response.getBody().isSuccess()) {
                    log.info("Successfully fetched {} Facebook orders",
                            response.getBody().getOrderCount());
                    return response.getBody();
                }

                log.warn("Facebook API returned unsuccessful response: {}",
                        response.getBody() != null ? response.getBody().getMessage() : "null");

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
            String healthUrl = baseUrl + "/health";
            ResponseEntity<String> response = restTemplate.exchange(
                    healthUrl,
                    HttpMethod.GET,
                    createHttpEntity(),
                    String.class
            );
            return response.getStatusCode().is2xxSuccessful();
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