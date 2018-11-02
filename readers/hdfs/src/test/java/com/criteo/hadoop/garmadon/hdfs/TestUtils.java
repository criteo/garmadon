package com.criteo.hadoop.garmadon.hdfs;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TestUtils {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    public static Instant instantFromDate(String timeString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(timeString));

        return Instant.from(localDateTime.toInstant(ZoneOffset.UTC));
    }

    public static LocalDateTime localDateTimeFromDate(String when) {
        return LocalDateTime.ofInstant(instantFromDate(when), UTC_ZONE);
    }
}
