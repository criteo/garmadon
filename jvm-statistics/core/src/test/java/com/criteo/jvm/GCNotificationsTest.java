package com.criteo.jvm;

import org.junit.Assert;
import org.junit.Test;

import javax.management.Notification;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class GCNotificationsTest {
    @Test
    public void triggeredListenerOnGC() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        GCNotifications notif = new GCNotifications(GCNotificationsTest::handleNotification);
        notif.subscribe(s -> {
            Assert.assertEquals("GC", s);
            latch.countDown();
        });
        System.gc();
        Assert.assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
    }

    static void handleNotification(Notification notification, Object handback) {
        Consumer<String> printer = (Consumer<String>) handback;
        if (printer != null)
            printer.accept("GC");
    }
}
