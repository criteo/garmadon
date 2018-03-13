package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class G1GCTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void fullgc() {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(HeuristicsResultDB.Severity.SEVERE, result.severity);
            Assert.assertEquals(HeuristicsResultDB.Severity.SEVERE, result.score);
            Assert.assertEquals(G1GC.class, result.heuristicClass);
            Assert.assertEquals(3, result.getDetailCount());
            Assert.assertEquals("Timestamp", result.getDetail(0).name);
            Assert.assertEquals(String.valueOf(timestamp), result.getDetail(0).value);
            Assert.assertEquals("Collector", result.getDetail(1).name);
            Assert.assertEquals("G1 Old Generation", result.getDetail(1).value);
            Assert.assertEquals("Pause", result.getDetail(2).name);
            Assert.assertEquals("1234", result.getDetail(2).value);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        G1GC g1GC = new G1GC(mockDB);
        JVMStatisticsProtos.GCStatisticsData.Builder builder = JVMStatisticsProtos.GCStatisticsData.newBuilder()
                .setCollectorName("G1 Old Generation")
                .setPauseTime(1234)
                .setTimestamp(timestamp);
        JVMStatisticsProtos.GCStatisticsData gcStats = builder.build();
        g1GC.process(APPLICATION_ID, CONTAINER_ID, gcStats);

    }

}