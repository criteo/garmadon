package com.criteo.hadoop.garmadon.reader.metrics;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;

import java.lang.management.ManagementFactory;
import java.util.Enumeration;

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
    public void Prometheus_Collect_Kafka_metrics() {
        Enumeration<Collector.MetricFamilySamples> coll = CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (coll.hasMoreElements()) {
            Collector.MetricFamilySamples metric = coll.nextElement();
            System.out.println(metric.name);
            System.out.println(metric.help);
            System.out.println(metric.samples);

        }
        assertNotNull(CollectorRegistry.defaultRegistry.getSampleValue("garmadon_kafka_metrics",
                new String[]{"name", "hostname", "release"},
                new String[]{"IoStats", GarmadonReader.HOSTNAME, PrometheusHttpConsumerMetrics.RELEASE}));
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
