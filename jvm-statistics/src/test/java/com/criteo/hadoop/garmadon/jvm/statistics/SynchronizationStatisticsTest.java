package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsLog;
import org.junit.Assert;
import org.junit.Test;
import sun.management.HotspotRuntimeMBean;
import sun.management.counter.Counter;
import sun.management.counter.Units;
import sun.management.counter.Variability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SynchronizationStatisticsTest {

    @Test
    public void collect() throws Throwable {
        StatisticsLog sink = new StatisticsLog();
        SynchronizationStatistics stats = new SynchronizationStatistics(new TestHotspotRuntimeMXBean());
        stats.collect(sink);
        Assert.assertEquals("synclocks[contendedlockattempts=1, futilewakeups=2, parks=3, notifications=4, inflations=5, deflations=6, monextant=7]", sink.toString());
    }

    private static class TestHotspotRuntimeMXBean implements HotspotRuntimeMBean {
        @Override
        public long getSafepointCount() {
            return 0;
        }

        @Override
        public long getTotalSafepointTime() {
            return 0;
        }

        @Override
        public long getSafepointSyncTime() {
            return 0;
        }

        @Override
        public List<Counter> getInternalRuntimeCounters() {
            List<Counter> list = new ArrayList<>(Arrays.asList(
                    new TestCounter("sun.rt._sync_ContendedLockAttempts", 1),
                    new TestCounter("sun.rt._sync_FutileWakeups", 2),
                    new TestCounter("sun.rt._sync_Parks", 3),
                    new TestCounter("sun.rt._sync_Notifications", 4),
                    new TestCounter("sun.rt._sync_Inflations", 5),
                    new TestCounter("sun.rt._sync_Deflations", 6),
                    new TestCounter("sun.rt._sync_MonExtant", 7)
            ));
            return list;
        }
    }

    private static class TestCounter implements Counter {
        private final String name;
        private final long value;

        public TestCounter(String name, long value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Units getUnits() {
            return null;
        }

        @Override
        public Variability getVariability() {
            return null;
        }

        @Override
        public boolean isVector() {
            return false;
        }

        @Override
        public int getVectorLength() {
            return 0;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean isInternal() {
            return false;
        }

        @Override
        public int getFlags() {
            return 0;
        }
    }
}