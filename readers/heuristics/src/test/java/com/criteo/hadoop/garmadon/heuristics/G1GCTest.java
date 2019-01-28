package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

public class G1GCTest {
    private static final String APPLICATION_ID = "application_42";
    private static final String ATTEMPT_ID = "attempt_42";
    private static final String CONTAINER_PREFIX_ID = "container_42_";

    private HeuristicsResultDB mockDB;

    @Before
    public void setUp() {
        mockDB = Mockito.mock(HeuristicsResultDB.class);
    }

    @Test
    public void fullgc_single() {
        testFullGC(1, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals(CONTAINER_PREFIX_ID + "0", result.getDetail(0).name);
            Assert.assertEquals("Timestamp: 2018-06-05T13:40:41.354Z[UTC], pauseTime: 1234ms", result.getDetail(0).value);
        });
    }

    @Test
    public void fullgc_alot() {
        testFullGC(100, result -> {
            Assert.assertEquals(1, result.getDetailCount());
            Assert.assertEquals("Containers", result.getDetail(0).name);
            Assert.assertEquals("100", result.getDetail(0).value);
        });
    }

    private void testFullGC(int nbContainers, Consumer<HeuristicResult> assertDetails) {
        long timestamp = 1528206041354L;
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(APPLICATION_ID, result.getAppId());
            Assert.assertEquals(HeuristicsResultDB.Severity.SEVERE, result.getSeverity());
            Assert.assertEquals(HeuristicsResultDB.Severity.SEVERE, result.getScore());
            Assert.assertEquals(G1GC.class, result.getHeuristicClass());
            assertDetails.accept(result);
            return null;
        }).when(mockDB).createHeuristicResult(Matchers.any());

        G1GC g1GC = new G1GC(mockDB);
        JVMStatisticsEventsProtos.GCStatisticsData.Builder builder = JVMStatisticsEventsProtos.GCStatisticsData.newBuilder()
                .setCollectorName("G1 Old Generation")
                .setPauseTime(1234);
        JVMStatisticsEventsProtos.GCStatisticsData gcStats = builder.build();
        for (int i = 0; i < nbContainers; i++)
            g1GC.process(timestamp, APPLICATION_ID, ATTEMPT_ID, CONTAINER_PREFIX_ID + i, gcStats);
        g1GC.onAppCompleted(APPLICATION_ID, ATTEMPT_ID);
    }

}