package com.statemint;

import java.util.Locale;
import java.util.TimeZone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StateMint entry point.
 *
 * <p>The JVM is pinned to UTC and a fixed default locale so that a server's regional settings can
 * never change a date or a decimal separator. Statements always use the formats their template
 * declares; see docs/ARCHITECTURE.md, "Deployment and platforms".
 */
@SpringBootApplication
public class StatemintApplication {

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Locale.setDefault(Locale.ROOT);
        SpringApplication.run(StatemintApplication.class, args);
    }
}
