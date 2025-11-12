package com.guno.dataimport.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Platform Configuration - Enable/Disable individual platforms
 *
 * Usage in application.yml:
 * platform:
 *   facebook:
 *     enabled: true
 *   tiktok:
 *     enabled: true
 *   shopee:
 *     enabled: false
 */
@Configuration
@ConfigurationProperties(prefix = "platform")
@Data
public class PlatformConfig {

    private PlatformSettings facebook = new PlatformSettings();
    private PlatformSettings tiktok = new PlatformSettings();
    private PlatformSettings shopee = new PlatformSettings();

    @Data
    public static class PlatformSettings {
        private boolean enabled = true;
    }

    // Convenience methods
    public boolean isFacebookEnabled() {
        return facebook.isEnabled();
    }

    public boolean isTikTokEnabled() {
        return tiktok.isEnabled();
    }

    public boolean isShopeeEnabled() {
        return shopee.isEnabled();
    }

    public String getEnabledPlatforms() {
        StringBuilder sb = new StringBuilder();
        if (isFacebookEnabled()) sb.append("Facebook,");
        if (isTikTokEnabled()) sb.append("TikTok,");
        if (isShopeeEnabled()) sb.append("Shopee,");

        String result = sb.toString();
        return result.isEmpty() ? "None" : result.substring(0, result.length() - 1);
    }

    public int getEnabledCount() {
        int count = 0;
        if (isFacebookEnabled()) count++;
        if (isTikTokEnabled()) count++;
        if (isShopeeEnabled()) count++;
        return count;
    }

    public boolean hasAnyPlatformEnabled() {
        return isFacebookEnabled() || isTikTokEnabled() || isShopeeEnabled();
    }
}