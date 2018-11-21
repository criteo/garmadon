package com.criteo.hadoop.garmadon.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class LogGCNotificationsTest {

    private static final Pattern GC_PATTERN = Pattern.compile(".* occurred at .*, took \\d+ms");

    @Test
    public void getGCNotificationWithInfos() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        LogGCNotifications notif = new LogGCNotifications();
        notif.subscribe((t, s) -> {
            Assert.assertTrue(GC_PATTERN.matcher((String)s).find());
            latch.countDown();
        });
        System.gc();
        Assert.assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
    }
}