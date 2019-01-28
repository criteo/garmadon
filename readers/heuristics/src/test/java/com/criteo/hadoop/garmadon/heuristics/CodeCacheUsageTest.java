package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class CodeCacheUsageTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
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
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
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
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.getSeverity());
            Assert.assertEquals(HeuristicsResultDB.Severity.MODERATE, result.getScore());
            Assert.assertEquals(CodeCacheUsage.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());
        CodeCacheUsage codeCacheUsage = new CodeCacheUsage(mockDB);
        JVMStatisticsEventsProtos.JVMStatisticsData jvmStats = buildCodeCacheData(63 * 1024 * 1024, 64 * 1024 * 1024);
        for (int i = 0; i < nbContainers; i++) {
            codeCacheUsage.process(System.currentTimeMillis(), APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, jvmStats);
            codeCacheUsage.onContainerCompleted(APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i);
        }
        codeCacheUsage.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

    private JVMStatisticsEventsProtos.JVMStatisticsData buildCodeCacheData(long used, long max) {
        JVMStatisticsEventsProtos.JVMStatisticsData.Builder builder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder = builder.addSectionBuilder().setName("code");
        sectionBuilder.addPropertyBuilder().setName("used").setValue(String.valueOf(used));
        sectionBuilder.addPropertyBuilder().setName("max").setValue(String.valueOf(max));
        return builder.build();
    }

}