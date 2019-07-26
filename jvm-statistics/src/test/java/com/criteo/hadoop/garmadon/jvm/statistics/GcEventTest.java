package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.GcEvent;
import org.junit.Assert;
import org.junit.Test;

public class GcEventTest {

    @Test
    public void isTooOld() {
        GcEvent gcEvent = new GcEvent(100L, 1000L);
        Assert.assertFalse(gcEvent.isTooOld(250));
        Assert.assertFalse(gcEvent.isTooOld(1000));
        Assert.assertTrue(gcEvent.isTooOld(2500));
    }

    @Test
    public void getPauseDuration() {
        GcEvent gcEvent = new GcEvent(100L, 1000L);
        Assert.assertEquals(gcEvent.getPauseDuration(), 900);
    }

    @Test
    public void getPauseDurationSince() {
        GcEvent gcEvent = new GcEvent(100L, 1000L);
        Assert.assertEquals(gcEvent.getPauseDurationSince(50), 900);
        Assert.assertEquals(gcEvent.getPauseDurationSince(100), 900);
        Assert.assertEquals(gcEvent.getPauseDurationSince(700), 300);
        Assert.assertEquals(gcEvent.getPauseDurationSince(1000), 0);
        Assert.assertEquals(gcEvent.getPauseDurationSince(1200), 0);
    }
}