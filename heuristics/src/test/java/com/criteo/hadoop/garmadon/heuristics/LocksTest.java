package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class LocksTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void low_contention_rate() {
        testContentionRate(HeuristicsResultDB.Severity.LOW, 10, 21);
    }

    @Test
    public void moderate_contention_rate() {
        testContentionRate(HeuristicsResultDB.Severity.MODERATE, 10, 61);
    }

    @Test
    public void severe_contention_rate() {
        testContentionRate(HeuristicsResultDB.Severity.SEVERE, 10, 111);
    }

    @Test
    public void critical_contention_rate() {
        testContentionRate(HeuristicsResultDB.Severity.CRITICAL, 10, 511);
    }

    private void testContentionRate(int severity, int count1, int count2) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(severity, result.severity);
            Assert.assertEquals(severity, result.score);
            Assert.assertEquals(Locks.class, result.heuristicClass);
            Assert.assertEquals(4, result.getDetailCount());
            Assert.assertEquals("Last contended count", result.getDetail(0).name);
            Assert.assertEquals(String.valueOf(count1), result.getDetail(0).value);
            Assert.assertEquals("Last timestamp", result.getDetail(1).name);
            Assert.assertEquals(String.valueOf(timestamp), result.getDetail(1).value);
            Assert.assertEquals("Current contended count", result.getDetail(2).name);
            Assert.assertEquals(String.valueOf(count2), result.getDetail(2).value);
            Assert.assertEquals("Current timestamp", result.getDetail(3).name);
            Assert.assertEquals(String.valueOf(timestamp+1000), result.getDetail(3).value);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Locks locks = new Locks(mockDB);
        locks.process(APPLICATION_ID, CONTAINER_ID, buildSynclocksData(count1, timestamp));
        locks.process(APPLICATION_ID, CONTAINER_ID, buildSynclocksData(count2, timestamp+1000));
    }

    JVMStatisticsProtos.JVMStatisticsData buildSynclocksData(int count, long timestamp) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        builder.setTimestamp(timestamp);
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("synclocks");
        sectionBuilder.addPropertyBuilder().setName("contendedlockattempts").setValue(String.valueOf(count));
        return builder.build();
    }

}