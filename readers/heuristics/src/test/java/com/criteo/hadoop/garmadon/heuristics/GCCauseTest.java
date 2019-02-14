package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static com.criteo.hadoop.garmadon.heuristics.GCCause.ERGONOMICS;
import static com.criteo.hadoop.garmadon.heuristics.GCCause.METADATA_THRESHOLD;

public class GCCauseTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void metadata_gc_threshold_single() {
        testCause(METADATA_THRESHOLD, 1, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals(METADATA_THRESHOLD + ": 1, " + ERGONOMICS + ": 0", result.getDetail(0).value);
        });
    }

    @Test
    public void metadata_gc_threshold_alot() {
        testCause(METADATA_THRESHOLD, 100, result -> {
            Assert.assertEquals(2, result.getDetailCount());
            Assert.assertEquals(METADATA_THRESHOLD, result.getDetail(0).name);
            Assert.assertEquals("100", result.getDetail(0).value);
            Assert.assertEquals(ERGONOMICS, result.getDetail(1).name);
            Assert.assertEquals("0", result.getDetail(1).value);
        });
    }

    @Test
    public void ergonomics_single() {
        testCause(ERGONOMICS, 1, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals(METADATA_THRESHOLD + ": 0, " + ERGONOMICS + ": 1", result.getDetail(0).value);
        });
    }

    @Test
    public void ergonomics_alot() {
        testCause(ERGONOMICS, 100, result -> {
            Assert.assertEquals(2, result.getDetailCount());
            Assert.assertEquals(METADATA_THRESHOLD, result.getDetail(0).name);
            Assert.assertEquals("0", result.getDetail(0).value);
            Assert.assertEquals(ERGONOMICS, result.getDetail(1).name);
            Assert.assertEquals("100", result.getDetail(1).value);
        });
    }

    private void testCause(String cause, int nbContainers, Consumer<HeuristicResult> assertDetails) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.getSeverity());
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.getScore());
            Assert.assertEquals(GCCause.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        GCCause gcCause = new GCCause(mockDB);
        JVMStatisticsEventsProtos.GCStatisticsData.Builder builder = JVMStatisticsEventsProtos.GCStatisticsData.newBuilder()
                .setCause(cause)
                .setCollectorName("PS MarkSweep")
                .setPauseTime(1234);
        JVMStatisticsEventsProtos.GCStatisticsData gcStats = builder.build();
        for (int i = 0; i < nbContainers; i++)
            gcCause.process(timestamp, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, gcStats);
        gcCause.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }
}
