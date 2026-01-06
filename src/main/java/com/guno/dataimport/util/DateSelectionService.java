package com.guno.dataimport.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Date Selection Service - Smart date selection with grace period
 *
 * PROBLEM SOLVED:
 * When day changes at 00:00, APIs (TikTok/Facebook) may still be processing
 * previous day's orders. Switching immediately causes data loss.
 *
 * SOLUTION:
 * Grace period 00:00 - 02:00 continues collecting previous day's data.
 * After 02:00, switch to current day.
 *
 * EXAMPLE:
 * 23:55 Dec 30 → Collect 2025-12-30 ✅
 * 00:05 Dec 31 → Collect 2025-12-30 ✅ (Grace period)
 * 01:30 Dec 31 → Collect 2025-12-30 ✅ (Grace period)
 * 02:05 Dec 31 → Collect 2025-12-31 ✅ (Switched)
 *
 * @author Data Import Team
 * @since 2025-12-31
 */
@Component
@Slf4j
public class DateSelectionService {

    @Value("${date-selection.cutoff-hour:2}")
    private int cutoffHour;

    @Value("${date-selection.timezone:Asia/Ho_Chi_Minh}")
    private String timezoneId;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Get collection date based on current time and grace period
     *
     * @return Date string (yyyy-MM-dd) - Yesterday if in grace period, today otherwise
     */
    public String getCollectionDate() {
        ZoneId zone = getZoneId();
        LocalDateTime now = LocalDateTime.now(zone);
        LocalDate dateToCollect;

        boolean inGracePeriod = now.getHour() < cutoffHour;

        if (inGracePeriod) {
            // Grace period: Use yesterday's date
            dateToCollect = now.toLocalDate().minusDays(1);
            logGracePeriod(now, dateToCollect, zone);
        } else {
            // Normal period: Use today's date
            dateToCollect = now.toLocalDate();
            log.info("📅 Collection Date: {} (TODAY) at {} {}",
                    dateToCollect.format(DATE_FORMATTER),
                    now.format(TIME_FORMATTER),
                    zone.getId());
        }

        return dateToCollect.format(DATE_FORMATTER);
    }

    /**
     * Check if currently in grace period
     *
     * @return true if before cutoff time
     */
    public boolean isInGracePeriod() {
        LocalDateTime now = LocalDateTime.now(getZoneId());
        return now.getHour() < cutoffHour;
    }

    /**
     * Get minutes until next cutoff
     *
     * @return Minutes remaining until cutoff
     */
    public long getMinutesUntilCutoff() {
        ZoneId zone = getZoneId();
        LocalDateTime now = LocalDateTime.now(zone);
        LocalDateTime cutoff = now.toLocalDate().atTime(cutoffHour, 0);

        if (now.isAfter(cutoff)) {
            cutoff = cutoff.plusDays(1);
        }

        return java.time.Duration.between(now, cutoff).toMinutes();
    }

    /**
     * Get current time in configured timezone
     *
     * @return Current LocalDateTime
     */
    public LocalDateTime getCurrentTime() {
        return LocalDateTime.now(getZoneId());
    }

    /**
     * Get formatted current time
     *
     * @return Formatted time (HH:mm:ss)
     */
    public String getCurrentTimeFormatted() {
        return getCurrentTime().format(TIME_FORMATTER);
    }

    /**
     * Get cutoff hour configuration
     *
     * @return Cutoff hour (0-23)
     */
    public int getCutoffHour() {
        return cutoffHour;
    }

    /**
     * Get timezone ID configuration
     *
     * @return Timezone ID
     */
    public String getTimezoneId() {
        return timezoneId;
    }

    /**
     * Log current configuration (useful for startup)
     */
    public void logConfiguration() {
        log.info("📅 Date Selection Service Configuration:");
        log.info("   ├─ Cutoff Hour: {:02d}:00", cutoffHour);
        log.info("   ├─ Timezone: {}", timezoneId);
        log.info("   ├─ Current Time: {}", getCurrentTimeFormatted());
        log.info("   ├─ In Grace Period: {}", isInGracePeriod() ? "YES" : "NO");
        log.info("   └─ Collection Date: {}", getCollectionDate());
    }

    // ================================
    // PRIVATE HELPERS
    // ================================

    private ZoneId getZoneId() {
        try {
            return ZoneId.of(timezoneId);
        } catch (Exception e) {
            log.warn("Invalid timezone '{}', using Asia/Ho_Chi_Minh", timezoneId);
            return ZoneId.of("Asia/Ho_Chi_Minh");
        }
    }

    private void logGracePeriod(LocalDateTime now, LocalDate dateToCollect, ZoneId zone) {
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║            GRACE PERIOD ACTIVE                             ║");
        log.info("╠════════════════════════════════════════════════════════════╣");
        log.info("║  Current Time:    {} {}              ║",
                now.format(TIME_FORMATTER),
                String.format("%-20s", zone.getId()));
        log.info("║  Cutoff Time:     {:02d}:00:00 {}              ║",
                cutoffHour,
                String.format("%-20s", zone.getId()));
        log.info("║  Collection Date: {} (YESTERDAY)                   ║",
                dateToCollect.format(DATE_FORMATTER));
        log.info("║  Reason:          Allowing APIs to complete prev day      ║");
        log.info("║  Next Switch:     In {} minutes                          ║",
                String.format("%-3d", getMinutesUntilCutoff()));
        log.info("╚════════════════════════════════════════════════════════════╝");
    }
}