package com.criteo.hadoop.garmadon.jvm.statistics;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class HotSpotMXBeanStatisticsTest {
    @Test
    public void parseLinuxFSCacheSize() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader("Buffers:         1033144 kB\n" +
                "Cached:         20360680 kB\n" +
                "SwapCached:            0 kB\n"));
        long size = HotSpotMXBeanStatistics.LinuxMemInfoWrapperOperatingSystemMXBean.parseLinuxFSCacheSize(reader);
        Assert.assertEquals(20360680L*1024, size);
    }
}