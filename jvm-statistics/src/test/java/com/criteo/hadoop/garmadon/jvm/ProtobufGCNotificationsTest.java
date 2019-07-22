package com.criteo.hadoop.garmadon.jvm;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.hamcrest.MatcherAssert;
import org.hamcrest.text.MatchesPattern;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ProtobufGCNotificationsTest {

    private static final Pattern GC_PATTERN = Pattern.compile("\\{\\s+\"collector_name\":\\s+\".*\"," +
            "\\s+\"pause_time\":\\s+\"\\d+\"," +
            "\\s+\"cause\":\\s+\"System\\.gc\\(\\)\"," +
            "\\s+\"eden_before\":\\s+\"\\d+\"," +
            "\\s+\"eden_after\":\\s+\"\\d+\"," +
            "\\s+\"survivor_before\":\\s+\"\\d+\"," +
            "\\s+\"survivor_after\":\\s+\"\\d+\"," +
            "\\s+\"old_before\":\\s+\"\\d+\"," +
            "\\s+\"old_after\":\\s+\"\\d+\"," +
            "\\s+\"code_before\":\\s+\"\\d+\"," +
            "\\s+\"code_after\":\\s+\"\\d+\"," +
            "\\s+\"metaspace_before\":\\s+\"\\d+\"," +
            "\\s+\"metaspace_after\":\\s+\"\\d+\"," +
            "\\s+\"gc_pause_ratio_1_min\":\\s+\\d+\\.\\d+\\s+\\}", Pattern.DOTALL);

    @Test
    public void getGCNotificationWithInfos() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ProtobufGCNotifications notif = new ProtobufGCNotifications();
        notif.subscribe((timestamp, stats) -> {
            JsonFormat.Printer printer = JsonFormat.printer()
                    .includingDefaultValueFields()
                    .preservingProtoFieldNames();
            try {
                String s = printer.print((MessageOrBuilder) stats);
                MatcherAssert.assertThat(s, MatchesPattern.matchesPattern(GC_PATTERN));
                latch.countDown();
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
        System.gc();
        Assert.assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void computeTotalPauseTime() {
        List<GcEvent> gcEvents = new ArrayList<>();
        gcEvents.add(new GcEvent(100, 1000));
        gcEvents.add(new GcEvent(1050, 1750));
        gcEvents.add(new GcEvent(62500, 65000));

        long totalPauseTime = ProtobufGCNotifications.computeTotalPauseTime(gcEvents, new GcEvent(67500, 69000));
        Assert.assertEquals(4000, totalPauseTime);
    }
}
