package com.criteo.hadoop.garmadon.hdfs.monitoring;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Enumeration;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrometheusMetricsTest {

    @Before
    public void setup() {
        PrometheusMetrics.clearCollectors();
    }

    @After
    public void tearDown() {
        PrometheusMetrics.clearCollectors();
    }

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

    @Test
    public void shouldClearAllCollectorsForPartition() {
        createCollectorsForEventPartition("event_1", 0);
        createCollectorsForEventPartition("event_1", 1);
        createCollectorsForEventPartition("event_1", 2);

        createCollectorsForEventPartition("event_2", 0);
        createCollectorsForEventPartition("event_2", 1);
        createCollectorsForEventPartition("event_2", 2);

        checkLabelExist("partition", "0");

        PrometheusMetrics.clearPartitionCollectors(0);

        checkLabelNotExist("partition", "0");
    }

    @Test
    public void shouldRegisterCollectorOnlyOnce() {
        createCollectorsForEventPartition("event_1", 0);
        createCollectorsForEventPartition("event_1", 0);
        createCollectorsForEventPartition("event_2", 1);
        createCollectorsForEventPartition("event_2", 1);

        assertEquals(2, PrometheusMetrics.getRegisteredCollectors().size());

        PrometheusMetrics.getRegisteredCollectors().get(0)
            .forEach((collectors, childLabels) -> assertEquals(1, childLabels.size()));

        PrometheusMetrics.getRegisteredCollectors().get(1)
            .forEach((collectors, childLabels) -> assertEquals(1, childLabels.size()));
    }

    @Test
    public void shouldNotFailWhenCleaningNonAlreadyRegisteredPartition() {
        PrometheusMetrics.clearPartitionCollectors(0);
    }

    private void checkLabelExist(String label, String value) {
        try {
            checkLabelNotExist(label, value);
        } catch (AssertionError e) {
            //label exists means checkLabelNotExist fails
            //so we are good here
            return;
        }
        fail("expected to find a collector with label " + label + "{" + value + "}");
    }

    private void checkLabelNotExist(String label, String value) {
        Enumeration<Collector.MetricFamilySamples> samples = CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (samples.hasMoreElements()) {
            Collector.MetricFamilySamples thoseSamples = samples.nextElement();
            thoseSamples.samples.forEach(sample -> {
                int idx = sample.labelNames.indexOf(label);
                if (idx >= 0) {
                    assertThat(sample.labelValues.get(idx), not(value));
                }
            });
        }
    }

    private void createCollectorsForEventPartition(String eventName, int partition) {
        PrometheusMetrics.currentRunningOffsetsGauge(eventName, partition);
        PrometheusMetrics.checkpointFailuresCounter(eventName, partition);
        PrometheusMetrics.checkpointSuccessesCounter(eventName, partition);
        PrometheusMetrics.hearbeatsSentCounter(eventName, partition);
        PrometheusMetrics.latestCommittedOffsetGauge(eventName, partition);
        PrometheusMetrics.latestCommittedTimestampGauge(eventName, partition);
        PrometheusMetrics.messageWritingFailuresCounter(eventName, partition);
        PrometheusMetrics.messageWrittenCounter(eventName, partition);
    }

}
