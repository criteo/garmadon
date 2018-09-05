package com.criteo.jvm.statistics;


import com.criteo.jvm.StatisticsLog;
import com.criteo.jvm.StatisticsSink;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;

public class MachineCpuStatisticsTest {

    @Test
    public void collectCpuTicks() {
        long[] prevTicks = new long[8];
        long[] ticks = new long[] {12, 34, 56, 78, 90, 21, 43, 65, 0};
        StatisticsLog sink = new StatisticsLog();
        MachineCpuStatistics.collectCpuTicks(ticks, prevTicks, sink);
        Assert.assertEquals("%user=4, %nice=10, %sys=17, %idle=23, %iowait=27, %irq=6, %softirq=13", sink.toString());
    }
}