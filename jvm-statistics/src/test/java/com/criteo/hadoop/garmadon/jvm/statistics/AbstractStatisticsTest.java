package com.criteo.hadoop.garmadon.jvm.statistics;

import org.junit.Assert;
import org.junit.Test;

public class AbstractStatisticsTest {

    @Test
    public void computeCpupercentage() throws Throwable {
        float cpuPercent = CpuStatistics.computeCpuPercentage(0, 1_000, 2, 0, 1000);
        Assert.assertEquals(50, cpuPercent, 0);

        cpuPercent = CpuStatistics.computeCpuPercentage(0, 100000, 12, 0, 10000);
        Assert.assertEquals(83.33333f, cpuPercent, 0);
    }
}