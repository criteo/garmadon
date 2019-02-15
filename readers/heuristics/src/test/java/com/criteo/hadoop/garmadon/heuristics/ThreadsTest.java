package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class ThreadsTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void low_created_threads_single() {
        testCreatedThreads(HeuristicsResultDB.Severity.LOW, 180, 1, getAssertDetailsSingle(180));
    }

    @Test
    public void low_created_threads_alot() {
        testCreatedThreads(HeuristicsResultDB.Severity.LOW, 180, 100, this::assertDetailsALot);
    }

    @Test
    public void mass_created_threads_single() {
        testCreatedThreads(HeuristicsResultDB.Severity.MODERATE, 1800, 1, getAssertDetailsSingle(1800));
    }

    @Test
    public void mass_created_threads_alot() {
        testCreatedThreads(HeuristicsResultDB.Severity.MODERATE, 1800, 100, this::assertDetailsALot);
    }

    private Consumer<HeuristicResult> getAssertDetailsSingle(int total) {
        return result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals("Max count threads: 12, Total threads: " + total, result.getDetail(0).value);
        };
    }

    private void assertDetailsALot(HeuristicResult result) {
        Assert.assertEquals(1, result.getDetailCount());
        Assert.assertEquals("Containers", result.getDetail(0).name);
        Assert.assertEquals("100", result.getDetail(0).value);
    }

    private void testCreatedThreads(int severity, int total, int nbContainers, Consumer<HeuristicResult> assertDetails) {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(severity, result.getSeverity());
            Assert.assertEquals(severity, result.getScore());
            Assert.assertEquals(Threads.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Threads threads = new Threads(mockDB);
        for (int i = 0; i < nbContainers; i++) {
            threads.process(System.currentTimeMillis(), APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildThreadData(12, 6, 12));
            threads.process(System.currentTimeMillis(), APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, buildThreadData(12, 6, total));
            threads.onContainerCompleted(APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i);
        }
        threads.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

    private JVMStatisticsEventsProtos.JVMStatisticsData buildThreadData(int count, int daemon, int total) {
        JVMStatisticsEventsProtos.JVMStatisticsData.Builder builder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("threads");
        sectionBuilder.addPropertyBuilder().setName("count").setValue(String.valueOf(count));
        sectionBuilder.addPropertyBuilder().setName("daemon").setValue(String.valueOf(daemon));
        sectionBuilder.addPropertyBuilder().setName("total").setValue(String.valueOf(total));
        return builder.build();
    }

}