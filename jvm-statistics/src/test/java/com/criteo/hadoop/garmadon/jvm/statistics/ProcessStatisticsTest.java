package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsLog;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;

public class ProcessStatisticsTest {

    @Test
    public void collectStatus() throws IOException {
        doCollectFromFile("/status.txt", "ctxtswitches=4, interrupts=67");
    }

    @Test

    public void collectIO() throws IOException {
        doCollectFromFile("/io.txt", "read=2, written=36");
    }

    private void doCollectFromFile(String fileName, String expected) throws IOException {
        StatisticsLog sink = new StatisticsLog();
        try (RandomAccessFile raf = new RandomAccessFile(ProcessStatisticsTest.class.getResource(fileName).getFile(), "r")) {
            ProcessStatistics.collectStatus(raf, sink);
        }
        Assert.assertEquals(expected, sink.toString());

    }
}