package com.guno.dataimport;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Main Spring Boot Application
 */
@SpringBootApplication
@EnableScheduling
public class DataImportApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataImportApplication.class, args);
    }
}