package com.criteo.hadoop.garmadon.jvm;

import org.junit.Assert;
import org.junit.Test;

import javax.management.Notification;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class GCNotificationsTest {
    @Test
    public void triggeredListenerOnGC() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        GCNotifications notif = new GCNotifications(GCNotificationsTest::handleNotification);
        notif.subscribe((t, s) -> {
            Assert.assertEquals("GC", s);
            latch.countDown();
        });
        System.gc();
        Assert.assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
    }

    static void handleNotification(Notification notification, Object handback) {
        BiConsumer<Long, String> printer = (BiConsumer<Long, String>) handback;
        if (printer != null)
            printer.accept(System.currentTimeMillis(), "GC");
    }
}
