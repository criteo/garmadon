package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class HeapUsageTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void heap_empty_single() {
        testHeapUsage(1, HeuristicsResultDB.Severity.SEVERE, 2 * 1024 * 1024, 512 * 1024 * 1024, getAssertDetailsSingle(99));
    }

    @Test
    public void heap_empty_alot() {
        testHeapUsage(100, HeuristicsResultDB.Severity.SEVERE, 2 * 1024 * 1024, 512 * 1024 * 1024, this::assertDetailsALot);
    }

    @Test
    public void heap_half_single() {
        testHeapUsage(1, HeuristicsResultDB.Severity.MODERATE, 250 * 1024 * 1024, 512 * 1024 * 1024, getAssertDetailsSingle(51));
    }

    @Test
    public void heap_half_alot() {
        testHeapUsage(100, HeuristicsResultDB.Severity.MODERATE, 250 * 1024 * 1024, 512 * 1024 * 1024, this::assertDetailsALot);
    }

    @Test
    public void heap_high_single() {
        testHeapUsage(1, HeuristicsResultDB.Severity.LOW, 350 * 1024 * 1024, 512 * 1024 * 1024, getAssertDetailsSingle(31));
    }

    @Test
    public void heap_high_alot() {
        testHeapUsage(100, HeuristicsResultDB.Severity.LOW, 350 * 1024 * 1024, 512 * 1024 * 1024, this::assertDetailsALot);
    }

    private Consumer<HeuristicResult> getAssertDetailsSingle(int ratio) {
        return result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals("unused memory %: " + ratio, result.getDetail(0).value);
        };
    }

    private void assertDetailsALot(HeuristicResult result) {
        Assert.assertEquals(1, result.getDetailCount());
        Assert.assertEquals("Containers", result.getDetail(0).name);
        Assert.assertEquals("100", result.getDetail(0).value);

    }

    private void testHeapUsage(int nbContainers, int severity, long used, long max, Consumer<HeuristicResult> assertDetails) {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(severity, result.getSeverity());
            Assert.assertEquals(severity, result.getScore());
            Assert.assertEquals(HeapUsage.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        HeapUsage heapUsage = new HeapUsage(mockDB);
        JVMStatisticsEventsProtos.JVMStatisticsData jvmStats = buildHeapData(used, max);
        for (int i = 0; i < nbContainers; i++) {
            heapUsage.process(System.currentTimeMillis(), APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, jvmStats);
            heapUsage.onContainerCompleted(APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i);
        }
        heapUsage.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

    private JVMStatisticsEventsProtos.JVMStatisticsData buildHeapData(long used, long max) {
        JVMStatisticsEventsProtos.JVMStatisticsData.Builder builder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("heap");
        sectionBuilder.addPropertyBuilder().setName("used").setValue(String.valueOf(used));
        sectionBuilder.addPropertyBuilder().setName("max").setValue(String.valueOf(max));
        sectionBuilder = builder.addSectionBuilder().setName("gc(PS Scavenge)");
        sectionBuilder.addPropertyBuilder().setName("count").setValue("1");
        return builder.build();
    }
}