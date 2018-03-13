package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class GCCauseTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void metadata_gc_threshold() {
        testCause("Metadata GC Threshold");
    }
    @Test
    public void ergonomics() {
        testCause("Ergonomics");
    }

    private void testCause(String cause) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.severity);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.score);
            Assert.assertEquals(GCCause.class, result.heuristicClass);
            Assert.assertEquals(4, result.getDetailCount());
            Assert.assertEquals("Timestamp", result.getDetail(0).name);
            Assert.assertEquals(String.valueOf(timestamp), result.getDetail(0).value);
            Assert.assertEquals("Collector", result.getDetail(1).name);
            Assert.assertEquals("PS MarkSweep", result.getDetail(1).value);
            Assert.assertEquals("Pause", result.getDetail(2).name);
            Assert.assertEquals("1234", result.getDetail(2).value);
            Assert.assertEquals("Cause", result.getDetail(3).name);
            Assert.assertEquals(cause, result.getDetail(3).value);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        GCCause gcCause = new GCCause(mockDB);
        JVMStatisticsProtos.GCStatisticsData.Builder builder = JVMStatisticsProtos.GCStatisticsData.newBuilder()
                .setCause(cause)
                .setCollectorName("PS MarkSweep")
                .setPauseTime(1234)
                .setTimestamp(timestamp);
        JVMStatisticsProtos.GCStatisticsData gcStats = builder.build();
        gcCause.process(APPLICATION_ID, CONTAINER_ID, gcStats);
    }
}
