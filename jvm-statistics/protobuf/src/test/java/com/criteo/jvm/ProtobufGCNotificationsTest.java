package com.criteo.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;

public class ProtobufGCNotificationsTest {

    private static final Pattern GC_PATTERN = Pattern.compile("collector_name: \".*\"\\s+" +
            "pause_time: \\d+\\s+" +
            "cause: \"System.gc\\(\\)\"\\s+" +
            ".*" +
            "old_after: \\d+\\s+" +
            "code_before: \\d+\\s+" +
            "code_after: \\d+\\s+" +
            "metaspace_before: \\d+\\s+" +
            "metaspace_after: \\d+\\s+", Pattern.DOTALL);

    @Test
    public void getGCNotificationWithInfos() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ProtobufGCNotifications notif = new ProtobufGCNotifications();
        notif.subscribe((timestamp, stats) -> {
            String s = stats.toString();
            System.out.println(s);
            assertThat(s, matchesPattern(GC_PATTERN));
            latch.countDown();
        });
        System.gc();
        Assert.assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
    }
}