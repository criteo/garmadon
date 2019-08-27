package com.criteo.hadoop.garmadon.hdfs.monitoring;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrometheusMetricsTest {

    @Test
    public void shouldSetLatestCommittedTimestampMaxSeenValue() {
        PrometheusMetrics.latestCommittedTimestampGauge("event", 0).set(0);
        PrometheusMetrics.latestCommittedTimestampGauge("event", 0).set(10);

        assertEquals(10, PrometheusMetrics.latestCommittedTimestampGauge("event", 0).get(), 0);

        PrometheusMetrics.latestCommittedTimestampGauge("event", 0).set(5);

        assertEquals(10, PrometheusMetrics.latestCommittedTimestampGauge("event", 0).get(), 0);

        PrometheusMetrics.latestCommittedTimestampGauge("event", 0).set(15);

        assertEquals(15, PrometheusMetrics.latestCommittedTimestampGauge("event", 0).get(), 0);
    }

}
