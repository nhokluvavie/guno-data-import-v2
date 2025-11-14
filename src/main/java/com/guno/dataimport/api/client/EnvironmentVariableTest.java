package com.guno.dataimport.api.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * TEST CLASS - Kiểm tra xem environment variables có load không
 * Chạy khi app start để xem log
 */
@Component
@Profile("prod")
@Slf4j
public class EnvironmentVariableTest implements CommandLineRunner {

    @Value("${api.facebook.headers.X-API-Key:NOT_SET}")
    private String facebookApiKey;

    @Value("${api.tiktok.headers.X-API-Key:NOT_SET}")
    private String tiktokApiKey;

    @Value("${api.facebook.base-url:NOT_SET}")
    private String facebookUrl;

    @Value("${api.tiktok.base-url:NOT_SET}")
    private String tiktokUrl;

    @Value("${api.facebook.page-size:0}")
    private int facebookPageSize;

    @Value("${api.tiktok.page-size:0}")
    private int tiktokPageSize;

    @Value("${FACEBOOK_API_KEY:NOT_SET_FROM_ENV}")
    private String rawEnvFacebookKey;

    @Value("${TIKTOK_API_KEY:NOT_SET_FROM_ENV}")
    private String rawEnvTiktokKey;

    @Override
    public void run(String... args) {
        log.info("╔═══════════════════════════════════════════════════════════════╗");
        log.info("║          ENVIRONMENT VARIABLES CHECK - PRODUCTION             ║");
        log.info("╚═══════════════════════════════════════════════════════════════╝");

        log.info("🔍 RAW Environment Variables:");
        log.info("   FACEBOOK_API_KEY from .env: {}", maskApiKey(rawEnvFacebookKey));
        log.info("   TIKTOK_API_KEY from .env:   {}", maskApiKey(rawEnvTiktokKey));

        log.info("");
        log.info("📋 Facebook API Config:");
        log.info("   Base URL:  {}", facebookUrl);
        log.info("   API Key:   {}", maskApiKey(facebookApiKey));
        log.info("   Page Size: {}", facebookPageSize);

        log.info("");
        log.info("📋 TikTok API Config:");
        log.info("   Base URL:  {}", tiktokUrl);
        log.info("   API Key:   {}", maskApiKey(tiktokApiKey));
        log.info("   Page Size: {}", tiktokPageSize);

        log.info("");
        log.info("✅ Status Check:");
        boolean facebookOk = !facebookApiKey.equals("NOT_SET") &&
                !facebookApiKey.isEmpty() &&
                facebookApiKey.length() > 10;
        boolean tiktokOk = !tiktokApiKey.equals("NOT_SET") &&
                !tiktokApiKey.isEmpty() &&
                tiktokApiKey.length() > 10;

        if (facebookOk) {
            log.info("   ✅ Facebook API Key: LOADED (length: {})", facebookApiKey.length());
        } else {
            log.error("   ❌ Facebook API Key: NOT LOADED or INVALID!");
            log.error("      Value: '{}'", facebookApiKey);
        }

        if (tiktokOk) {
            log.info("   ✅ TikTok API Key: LOADED (length: {})", tiktokApiKey.length());
        } else {
            log.error("   ❌ TikTok API Key: NOT LOADED or INVALID!");
            log.error("      Value: '{}'", tiktokApiKey);
        }

        if (!facebookOk || !tiktokOk) {
            log.error("");
            log.error("╔═══════════════════════════════════════════════════════════════╗");
            log.error("║  ⚠️  CONFIGURATION ERROR - API KEYS NOT LOADED PROPERLY      ║");
            log.error("╚═══════════════════════════════════════════════════════════════╝");
            log.error("");
            log.error("💡 Troubleshooting steps:");
            log.error("   1. Check .env.production file exists");
            log.error("   2. Check export commands in run-production.sh:");
            log.error("      export $(cat .env.production | xargs)");
            log.error("   3. Check Spring profile is 'prod':");
            log.error("      export SPRING_PROFILES_ACTIVE=prod");
            log.error("   4. Restart application");
        }

        log.info("╚═══════════════════════════════════════════════════════════════╝");
    }

    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.isEmpty() || apiKey.equals("NOT_SET") || apiKey.equals("NOT_SET_FROM_ENV")) {
            return apiKey;
        }

        if (apiKey.length() <= 8) {
            return "***";
        }

        String firstPart = apiKey.substring(0, 4);
        String lastPart = apiKey.substring(apiKey.length() - 4);
        return firstPart + "..." + lastPart + " (length: " + apiKey.length() + ")";
    }
}