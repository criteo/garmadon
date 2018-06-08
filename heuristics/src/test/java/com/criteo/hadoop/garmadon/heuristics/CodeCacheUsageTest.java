package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class CodeCacheUsageTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void code_cache_full_single() {
        testCodeCacheUsage(1, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID+"0", result.getDetail(0).name);
            Assert.assertEquals("max: 67108864kB, peak: 66060288kB", result.getDetail(0).value);
        });
    }

    @Test
    public void code_cache_full_alot() {
        testCodeCacheUsage(100, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals("Containers", result.getDetail(0).name);
            Assert.assertEquals("100", result.getDetail(0).value);

        });
    }

    private void testCodeCacheUsage(int nbContainers, Consumer<HeuristicResult> assertDetails) {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.appId);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.severity);
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.score);
            Assert.assertEquals(CodeCacheUsage.class, result.heuristicClass);
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        CodeCacheUsage codeCacheUsage = new CodeCacheUsage(mockDB);
        JVMStatisticsProtos.JVMStatisticsData jvmStats = buildCodeCacheData(63*1024*1024, 64*1024*1024);
        for (int i = 0; i < nbContainers; i++) {
            codeCacheUsage.process(APPLICATION_ID, CONTAINER_PREFIX_ID + i, jvmStats);
            codeCacheUsage.onContainerCompleted(APPLICATION_ID, CONTAINER_PREFIX_ID + i);
        }
        codeCacheUsage.onAppCompleted(APPLICATION_ID);
    }

    private JVMStatisticsProtos.JVMStatisticsData buildCodeCacheData(long used, long max) {
        JVMStatisticsProtos.JVMStatisticsData.Builder builder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("code");
        sectionBuilder.addPropertyBuilder().setName("used").setValue(String.valueOf(used));
        sectionBuilder.addPropertyBuilder().setName("max").setValue(String.valueOf(max));
        return builder.build();
    }

}