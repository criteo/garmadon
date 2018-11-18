package com.criteo.hadoop.garmadon.jvm.statistics;

import org.junit.Assert;
import org.junit.Test;

public class CpuStatisticsTest {

    @Test
    public void computeCpupercentage() throws Throwable {
        long cpuPercent = CpuStatistics.computeCpuPercentage(0, 1_000_000_000, 2, 0, 1000);
        Assert.assertEquals(50, cpuPercent);
    }
}