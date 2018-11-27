package com.criteo.hadoop.garmadon.hdfs;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TestUtils {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    /**
     * Build an instant from a formatted date string, interpreted as UTC
     * @param when      Time formatted as yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static Instant instantFromDate(String when) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(when));

        return Instant.from(localDateTime.toInstant(ZoneOffset.UTC));
    }

    /**
     * Build a LocalDateTime from a formatted date string, interpreted as UTC
     * @param when      Time formatted as yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static LocalDateTime localDateTimeFromDate(String when) {
        return LocalDateTime.ofInstant(instantFromDate(when), UTC_ZONE);
    }
}
