package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class CodeCacheUsageTest {
    private final String APPLICATION_ID = "application_42";
    private final String CONTAINER_ID = "container_42_42";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void code_cache_full() {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(CONTAINER_ID, result.containerId);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.severity);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.score);
            Assert.assertEquals(CodeCacheUsage.class, result.heuristicClass);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        CodeCacheUsage codeCacheUsage = new CodeCacheUsage(mockDB);
        JVMStatisticsProtos.JVMStatisticsData jvmStats = buildCodeCacheData(63*1024*1024, 64*1024*1024);
        codeCacheUsage.process(APPLICATION_ID, CONTAINER_ID, jvmStats);
    }

    private JVMStatisticsProtos.JVMStatisticsData buildCodeCacheData(long used, long max) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("code");
        sectionBuilder.addPropertyBuilder().setName("used").setValue(String.valueOf(used));
        sectionBuilder.addPropertyBuilder().setName("max").setValue(String.valueOf(max));
        return builder.build();
    }

}