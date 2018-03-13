package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class HeapUsageTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void heap_empty() {
        testHeapUsage(HeuristicsResultDB.Severity.SEVERE, 2*1024*1024, 512*1024*1024);
    }

    @Test
    public void heap_half() {
        testHeapUsage(HeuristicsResultDB.Severity.MODERATE, 250*1024*1024, 512*1024*1024);
    }

    @Test
    public void heap_high() {
        testHeapUsage(HeuristicsResultDB.Severity.LOW, 350*1024*1024, 512*1024*1024);
    }

    private void testHeapUsage(int severity, long used, long max) {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(severity, result.severity);
            Assert.assertEquals(severity, result.score);
            Assert.assertEquals(HeapUsage.class, result.heuristicClass);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        HeapUsage heapUsage = new HeapUsage(mockDB);
        JVMStatisticsProtos.JVMStatisticsData jvmStats = buildHeapData(used, max);
        heapUsage.process(APPLICATION_ID, CONTAINER_ID, jvmStats);
        heapUsage.onCompleted(APPLICATION_ID, CONTAINER_ID);
    }

    private JVMStatisticsProtos.JVMStatisticsData buildHeapData(long used, long max) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("heap");
        sectionBuilder.addPropertyBuilder().setName("used").setValue(String.valueOf(used));
        sectionBuilder.addPropertyBuilder().setName("max").setValue(String.valueOf(max));
        sectionBuilder = builder.addSectionBuilder().setName("gc(PS Scavenge)");
        sectionBuilder.addPropertyBuilder().setName("count").setValue("1");
        return builder.build();
    }
}