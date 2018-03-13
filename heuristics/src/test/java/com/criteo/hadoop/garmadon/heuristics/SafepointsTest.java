package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class SafepointsTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void low_safepoints_rate() {
        testSafepointRate(HeuristicsResultDB.Severity.LOW, 10, 14);
    }

    @Test
    public void moderate_safepoints_rate() {
        testSafepointRate(HeuristicsResultDB.Severity.MODERATE, 10, 16);
    }

    @Test
    public void severe_safepoints_rate() {
        testSafepointRate(HeuristicsResultDB.Severity.SEVERE, 10, 18);
    }

    @Test
    public void critical_safepoints_rate() {
        testSafepointRate(HeuristicsResultDB.Severity.CRITICAL, 10, 21);
    }

    private void testSafepointRate(int severity, int count1, int count2) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(severity, result.severity);
            Assert.assertEquals(severity, result.score);
            Assert.assertEquals(Safepoints.class, result.heuristicClass);
            Assert.assertEquals(4, result.getDetailCount());
            Assert.assertEquals("Last count", result.getDetail(0).name);
            Assert.assertEquals(String.valueOf(count1), result.getDetail(0).value);
            Assert.assertEquals("Last timestamp", result.getDetail(1).name);
            Assert.assertEquals(String.valueOf(timestamp), result.getDetail(1).value);
            Assert.assertEquals("Current count", result.getDetail(2).name);
            Assert.assertEquals(String.valueOf(count2), result.getDetail(2).value);
            Assert.assertEquals("Current timestamp", result.getDetail(3).name);
            Assert.assertEquals(String.valueOf(timestamp+1000), result.getDetail(3).value);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Safepoints safepoints = new Safepoints(mockDB);
        safepoints.process(APPLICATION_ID, CONTAINER_ID, buildSafepointData(count1, timestamp));
        safepoints.process(APPLICATION_ID, CONTAINER_ID, buildSafepointData(count2, timestamp+1000));
    }

    JVMStatisticsProtos.JVMStatisticsData buildSafepointData(int count, long timestamp) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        builder.setTimestamp(timestamp);
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("safepoints");
        sectionBuilder.addPropertyBuilder().setName("count").setValue(String.valueOf(count));
        return builder.build();
    }
}