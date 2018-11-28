package com.criteo.hadoop.garmadon.reader.metrics;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import io.prometheus.client.CollectorRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;

import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertNotNull;

public class PrometheusHttpConsumerMetricsTest {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName mbeanName;

    PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;


    @Before
    public void up() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        String objectName = "kafka.consumer:type=consumer-metrics,client-id=" + GarmadonReader.CONSUMER_ID;

        mbeanName = new ObjectName(objectName);
        KafkaMetrics mbean = new KafkaMetrics();
        server.registerMBean(mbean, mbeanName);

        prometheusHttpConsumerMetrics = new PrometheusHttpConsumerMetrics(0);
    }

    @After
    public void down() throws MBeanRegistrationException, InstanceNotFoundException {
        if (prometheusHttpConsumerMetrics != null) {
            prometheusHttpConsumerMetrics.terminate();
        }
        if (mbeanName != null) {
            server.unregisterMBean(mbeanName);
        }
    }

    @Test
    public void Prometheus_Collect_Default_Hotspot_Metrics() {
        assertNotNull(CollectorRegistry.defaultRegistry.getSampleValue("jvm_threads_started_total"));
    }

    @Test
    public void Prometheus_Collect_Kafka_metrics() throws MBeanException, IntrospectionException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException {
        PrometheusHttpConsumerMetrics.exposeKafkaMetrics();
        assertNotNull(CollectorRegistry.defaultRegistry.getSampleValue("garmadon_kafka_metrics",
                new String[]{"name", "hostname", "release"},
                new String[]{"IoStats", GarmadonReader.getHostname(), PrometheusHttpConsumerMetrics.RELEASE}));
    }

    static class KafkaMetrics implements KafkaMetricsMBean {
        private double ioStats = 10.0;

        @Override
        public double getIoStats() {
            return this.ioStats;
        }

        @Override
        public void setIoStats(double ioStats) {
            this.ioStats = ioStats;
        }
    }

    public static interface KafkaMetricsMBean {
        double getIoStats();

        void setIoStats(double message);
    }

}
