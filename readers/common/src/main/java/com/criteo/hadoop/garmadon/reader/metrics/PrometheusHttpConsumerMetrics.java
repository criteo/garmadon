package com.criteo.hadoop.garmadon.reader.metrics;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PrometheusHttpConsumerMetrics {
    public static final String RELEASE = Optional
            .ofNullable(PrometheusHttpConsumerMetrics.class.getPackage().getImplementationVersion())
            .orElse("1.0-SNAPSHOT")
            .replace(".", "_");

    public static final Counter GARMADON_READER_METRICS = Counter.build()
            .name("garmadon_reader_metrics").help("Garmadon reader metrics")
            .labelNames("name", "hostname", "release")
            .register();

    public static final Summary LATENCY_INDEXING_TO_ES = Summary.build()
            .name("garmadon_reader_duration").help("Duration in ms")
            .labelNames("name", "hostname", "release")
            .quantile(0.75, 0.01)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    public static final Counter.Child ISSUE_READING_GARMADON_MESSAGE_BAD_HEAD = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS
            .labels("issue_reading_garmadon_message_bad_head",
                    GarmadonReader.getHostname(),
                    PrometheusHttpConsumerMetrics.RELEASE);

    public static final Counter.Child ISSUE_READING_PROTO_HEAD = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("issue_reading_proto_head",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);

    public static final Counter.Child ISSUE_READING_PROTO_BODY = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("issue_reading_proto_body",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);


    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusHttpConsumerMetrics.class);
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    private static final Gauge BASE_KAFKA_METRICS_GAUGE = Gauge.build()
            .name("garmadon_kafka_metrics").help("Kafka producer metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    private static ObjectName oName;

    static {
        // Expose JMX, GCs, classloading, thread count, memory pool
        DefaultExports.initialize();
        try {
            oName = new ObjectName("kafka.consumer:type=consumer-metrics,client-id=" + GarmadonReader.CONSUMER_ID);
        } catch (MalformedObjectNameException e) {
            LOGGER.error("", e);
        }

        scheduler.scheduleAtFixedRate(() -> {
            exposeKafkaMetrics();
        }, 0, 30, TimeUnit.SECONDS);
    }

    private HTTPServer server;

    public PrometheusHttpConsumerMetrics(int prometheusPort) {
        try {
            server = new HTTPServer(prometheusPort);
        } catch (IOException e) {
            LOGGER.error("Failed to initialize Prometheus Http server: ", e);
        }
    }

    public void terminate() {
        if (server != null) {
            server.stop();
        }
        scheduler.shutdown();
    }

    protected static void exposeKafkaMetrics() {
        try {
            if (oName != null) {
                MBeanInfo info = MBS.getMBeanInfo(oName);
                MBeanAttributeInfo[] attrInfo = info.getAttributes();
                for (MBeanAttributeInfo attr : attrInfo) {
                    if (attr.isReadable() && attr.getType().equals("double")) {
                        BASE_KAFKA_METRICS_GAUGE.labels(attr.getName(), GarmadonReader.getHostname(), RELEASE)
                                .set((Double) MBS.getAttribute(oName, attr.getName()));
                    }
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException | MBeanException | AttributeNotFoundException e) {
            LOGGER.error("", e);
        }
    }
}
