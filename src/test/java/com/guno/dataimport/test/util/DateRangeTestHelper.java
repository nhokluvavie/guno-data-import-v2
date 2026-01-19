package com.guno.dataimport.test.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Date Range Test Helper
 * Utility for handling date range configuration in integration tests
 */
@Component
@Slf4j
@Data
public class DateRangeTestHelper {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Value("${test.data.use-date-range:false}")
    private boolean useDateRange;

    @Value("${test.data.date:}")
    private String singleDate;

    @Value("${test.data.start-date:}")
    private String startDate;

    @Value("${test.data.end-date:}")
    private String endDate;

    @Value("${test.data.delay-between-dates-ms:1000}")
    private long delayBetweenDatesMs;

    @Value("${test.data.continue-on-error:true}")
    private boolean continueOnError;

    @Value("${test.data.generate-summary:true}")
    private boolean generateSummary;

    @Value("${test.data.log-per-date-summary:true}")
    private boolean logPerDateSummary;

    /**
     * Get list of dates to process
     * @return List of dates in yyyy-MM-dd format
     */
    public List<String> getDatesToProcess() {
        List<String> dates = new ArrayList<>();

        if (!useDateRange) {
            // Single date mode
            dates.add(singleDate);
            log.info("📅 Using single date mode: {}", singleDate);
        } else {
            // Date range mode
            LocalDate start = LocalDate.parse(startDate, DATE_FORMATTER);
            LocalDate end = LocalDate.parse(endDate, DATE_FORMATTER);

            if (end.isBefore(start)) {
                log.error("❌ Invalid date range: end-date ({}) is before start-date ({})", endDate, startDate);
                throw new IllegalArgumentException("end-date must be >= start-date");
            }

            LocalDate current = start;
            while (!current.isAfter(end)) {
                dates.add(current.format(DATE_FORMATTER));
                current = current.plusDays(1);
            }

            log.info("📅 Using date range mode: {} to {} ({} days)", startDate, endDate, dates.size());
        }

        return dates;
    }

    /**
     * Sleep between date processing
     */
    public void delayBetweenDates() {
        if (delayBetweenDatesMs > 0) {
            try {
                log.debug("⏸️ Waiting {}ms before next date...", delayBetweenDatesMs);
                Thread.sleep(delayBetweenDatesMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Sleep interrupted");
            }
        }
    }

    /**
     * Check if should continue on error
     */
    public boolean shouldContinueOnError() {
        return continueOnError;
    }

    /**
     * Check if should generate summary
     */
    public boolean shouldGenerateSummary() {
        return generateSummary;
    }

    /**
     * Check if should log per-date summary
     */
    public boolean shouldLogPerDateSummary() {
        return logPerDateSummary;
    }

    /**
     * Get date range info for logging
     */
    public String getDateRangeInfo() {
        if (!useDateRange) {
            return String.format("Single Date: %s", singleDate);
        } else {
            long days = getDatesToProcess().size();
            return String.format("Date Range: %s to %s (%d days)", startDate, endDate, days);
        }
    }

    /**
     * Validate configuration
     */
    public void validateConfiguration() {
        if (!useDateRange) {
            if (singleDate == null || singleDate.isEmpty()) {
                throw new IllegalStateException("test.data.date must be configured when use-date-range=false");
            }
        } else {
            if (startDate == null || startDate.isEmpty()) {
                throw new IllegalStateException("test.data.start-date must be configured when use-date-range=true");
            }
            if (endDate == null || endDate.isEmpty()) {
                throw new IllegalStateException("test.data.end-date must be configured when use-date-range=true");
            }
        }
        log.info("✅ Date configuration validated: {}", getDateRangeInfo());
    }
}