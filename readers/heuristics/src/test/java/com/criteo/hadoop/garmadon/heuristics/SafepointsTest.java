package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class SafepointsTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void low_safepoints_rate_single() {
        testSafepointRate(HeuristicsResultDB.Severity.LOW, 10, 14, 1, getAssertDetailsSingle(4));
    }

    @Test
    public void low_safepoints_rate_alot() {
        testSafepointRate(HeuristicsResultDB.Severity.LOW, 10, 14, 100, this::assertDetailsALot);
    }

    @Test
    public void moderate_safepoints_rate_single() {
        testSafepointRate(HeuristicsResultDB.Severity.MODERATE, 10, 16, 1, getAssertDetailsSingle(6));
    }

    @Test
    public void moderate_safepoints_rate_alot() {
        testSafepointRate(HeuristicsResultDB.Severity.MODERATE, 10, 16, 100, this::assertDetailsALot);
    }

    @Test
    public void severe_safepoints_single() {
        testSafepointRate(HeuristicsResultDB.Severity.SEVERE, 10, 18, 1, getAssertDetailsSingle(8));
    }

    @Test
    public void severe_safepoints_alot() {
        testSafepointRate(HeuristicsResultDB.Severity.SEVERE, 10, 18, 100, this::assertDetailsALot);
    }

    @Test
    public void critical_safepoints_rate_single() {
        testSafepointRate(HeuristicsResultDB.Severity.CRITICAL, 10, 21, 1, getAssertDetailsSingle(11));
    }

    @Test
    public void critical_safepoints_rate_alot() {
        testSafepointRate(HeuristicsResultDB.Severity.CRITICAL, 10, 21, 100, this::assertDetailsALot);
    }

    private Consumer<HeuristicResult> getAssertDetailsSingle(int ratio) {
        return result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals("Max safepoint/s: " + ratio, result.getDetail(0).value);
        };
    }

    private void assertDetailsALot(HeuristicResult result) {
        Assert.assertEquals(1, result.getDetailCount());
        Assert.assertEquals("Containers", result.getDetail(0).name);
        Assert.assertEquals("100", result.getDetail(0).value);
    }

    private void testSafepointRate(int severity, int count1, int count2, int nbContainers, Consumer<HeuristicResult> assertDetails) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(severity, result.getSeverity());
            Assert.assertEquals(severity, result.getScore());
            Assert.assertEquals(Safepoints.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Safepoints safepoints = new Safepoints(mockDB);
        for (int i = 0; i < nbContainers; i++) {
            safepoints.process(timestamp, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildSafepointData(count1));
            safepoints.process(timestamp + 1000, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildSafepointData(count2));
            safepoints.onContainerCompleted(APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i);
        }
        safepoints.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

    JVMStatisticsEventsProtos.JVMStatisticsData buildSafepointData(int count) {
        JVMStatisticsEventsProtos.JVMStatisticsData.Builder builder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("safepoints");
        sectionBuilder.addPropertyBuilder().setName("count").setValue(String.valueOf(count));
        return builder.build();
    }
}