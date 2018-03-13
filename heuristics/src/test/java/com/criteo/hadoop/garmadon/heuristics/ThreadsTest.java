package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class ThreadsTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void mass_created_threads() {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.severity);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.score);
            Assert.assertEquals(Threads.class, result.heuristicClass);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        Threads threads = new Threads(mockDB);
        for (int i = 1; i < 150; i++)
            threads.process(APPLICATION_ID, CONTAINER_ID, buildThreadData(12, 6, 12*i));
        threads.onCompleted(APPLICATION_ID, CONTAINER_ID);
    }

    private JVMStatisticsProtos.JVMStatisticsData buildThreadData(int count, int daemon, int total) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("threads");
        sectionBuilder.addPropertyBuilder().setName("count").setValue(String.valueOf(count));
        sectionBuilder.addPropertyBuilder().setName("daemon").setValue(String.valueOf(daemon));
        sectionBuilder.addPropertyBuilder().setName("total").setValue(String.valueOf(total));
        return builder.build();
    }

}