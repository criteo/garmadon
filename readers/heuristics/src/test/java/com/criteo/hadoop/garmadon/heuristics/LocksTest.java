package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class LocksTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void low_contention_rate_single() {
        testContentionRate(HeuristicsResultDB.Severity.LOW, 10, 21, 1, getAssertDetailsSingle(11));
    }

    @Test
    public void low_contention_rate_alot() {
        testContentionRate(HeuristicsResultDB.Severity.LOW, 10, 21, 100, this::assertDetailsALot);
    }

    @Test
    public void moderate_contention_rate_single() {
        testContentionRate(HeuristicsResultDB.Severity.MODERATE, 10, 61, 1, getAssertDetailsSingle(51));
    }

    @Test
    public void moderate_contention_rate_alot() {
        testContentionRate(HeuristicsResultDB.Severity.MODERATE, 10, 61, 100, this::assertDetailsALot);
    }

    @Test
    public void severe_contention_rate_single() {
        testContentionRate(HeuristicsResultDB.Severity.SEVERE, 10, 111, 1, getAssertDetailsSingle(101));
    }

    @Test
    public void severe_contention_rate_alot() {
        testContentionRate(HeuristicsResultDB.Severity.SEVERE, 10, 111, 100, this::assertDetailsALot);
    }

    @Test
    public void critical_contention_rate_single() {
        testContentionRate(HeuristicsResultDB.Severity.CRITICAL, 10, 511, 1, getAssertDetailsSingle(501));
    }

    @Test
    public void critical_contention_rate_alot() {
        testContentionRate(HeuristicsResultDB.Severity.CRITICAL, 10, 511, 100, this::assertDetailsALot);
    }

    private Consumer<HeuristicResult> getAssertDetailsSingle(int ratio) {
        return result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals("Max contention/s: " + ratio, result.getDetail(0).value);
        };
    }

    private void assertDetailsALot(HeuristicResult result) {
        Assert.assertEquals(1, result.getDetailCount());
        Assert.assertEquals("Containers", result.getDetail(0).name);
        Assert.assertEquals("100", result.getDetail(0).value);
    }

    private void testContentionRate(int severity, int count1, int count2, int nbContainers, Consumer<HeuristicResult> assertDetails) {
        long timestamp = System.currentTimeMillis();
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(severity, result.getSeverity());
            Assert.assertEquals(severity, result.getScore());
            Assert.assertEquals(Locks.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Locks locks = new Locks(mockDB);
        for (int i = 0; i < nbContainers; i++) {
            locks.process(timestamp, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildSynclocksData(count1));
            locks.process(timestamp + 1000, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildSynclocksData(count2));
            locks.onContainerCompleted(APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i);
        }
        locks.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

    JVMStatisticsEventsProtos.JVMStatisticsData buildSynclocksData(int count) {
        JVMStatisticsEventsProtos.JVMStatisticsData.Builder builder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("synclocks");
        sectionBuilder.addPropertyBuilder().setName("contendedlockattempts").setValue(String.valueOf(count));
        return builder.build();
    }

}