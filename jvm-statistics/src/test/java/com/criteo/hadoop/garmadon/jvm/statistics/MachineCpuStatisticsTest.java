package com.criteo.hadoop.garmadon.jvm.statistics;


import com.criteo.hadoop.garmadon.jvm.StatisticsLog;
import org.junit.Assert;
import org.junit.Test;

public class MachineCpuStatisticsTest {

    @Test
    public void collectCpuTicks() {
        long[] prevTicks = new long[8];
        long[] ticks = new long[] {12, 34, 56, 78, 90, 21, 43, 65, 0};
        StatisticsLog sink = new StatisticsLog();
        MachineCpuStatistics.collectCpuTicks(ticks, prevTicks, sink);
        Assert.assertEquals("%user=4.0, %nice=10.0, %sys=17.0, %idle=23.0, %iowait=27.0, %irq=6.0, %softirq=13.0", sink.toString());
    }
}