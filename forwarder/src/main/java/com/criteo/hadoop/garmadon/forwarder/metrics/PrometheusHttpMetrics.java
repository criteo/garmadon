package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.criteo.hadoop.garmadon.forwarder.Forwarder;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
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
public class PrometheusHttpMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusHttpMetrics.class);
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    private static final Counter GARMADON_METRICS = Counter.build()
            .name("garmadon_metrics").help("Garmadon metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static final String RELEASE = Optional
            .ofNullable(PrometheusHttpMetrics.class.getPackage().getImplementationVersion())
            .orElse("1.0-SNAPSHOT")
            .replace(".", "_");

    private static final Counter.Child FOR_JOIN = GARMADON_METRICS.labels("garmadon_for_join",
            Forwarder.getHostname(),
            RELEASE);

    public static final Counter.Child EVENTS_RECEIVED = GARMADON_METRICS.labels("garmadon_events_received",
            Forwarder.getHostname(),
            RELEASE);
    public static final Counter.Child EVENTS_IN_ERROR = GARMADON_METRICS.labels("garmadon_events_in_error",
            Forwarder.getHostname(),
            RELEASE);
    public static final Counter.Child GREETINGS_RECEIVED = GARMADON_METRICS.labels("garmadon_greetings_received",
            Forwarder.getHostname(),
            RELEASE);
    public static final Counter.Child GREETINGS_IN_ERROR = GARMADON_METRICS.labels("garmadon_greetings_in_error",
            Forwarder.getHostname(),
            RELEASE);

    private static final Summary BASE_TYPE_SIZE_SUMMARY = Summary.build()
            .name("garmadon_event_size").help("Event sizes in bytes")
            .labelNames("eventType", "hostname", "release")
            .quantile(0.75, 0.01)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    private static final Gauge BASE_KAFKA_METRICS_GAUGE = Gauge.build()
            .name("garmadon_kafka_metrics").help("Kafka producer metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static HTTPServer server;

    private static ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    private static ObjectName oname;

    static {
        // Expose JMX, GCs, classloading, thread count, memory pool
        DefaultExports.initialize();
        FOR_JOIN.inc();
        try {
            oname = new ObjectName("kafka.producer:type=producer-metrics,client-id=" + Forwarder.PRODUCER_PREFIX_NAME + "." + Forwarder.getHostname());
        } catch (MalformedObjectNameException e) {
            LOGGER.error("", e);
        }
        scheduler.scheduleAtFixedRate(() -> {
            exposeKafkaMetrics();
        }, 30, 60, TimeUnit.SECONDS);
    }

    protected PrometheusHttpMetrics() {
        throw new UnsupportedOperationException();
    }

    public static void start(int prometheusPort) throws IOException {
        if (server == null) {
            server = new HTTPServer(prometheusPort);
        }
    }

    public static void stop() {
        if (server != null) {
            server.stop();
        }
        scheduler.shutdown();
    }

    public static Summary.Child eventSize(int type) {
        return BASE_TYPE_SIZE_SUMMARY.labels(GarmadonSerialization.getTypeName(type), Forwarder.getHostname(), RELEASE);
    }

    public static void exposeKafkaMetrics() {
        try {
            if (oname != null) {
                MBeanInfo info = MBS.getMBeanInfo(oname);
                MBeanAttributeInfo[] attrInfo = info.getAttributes();
                for (MBeanAttributeInfo attr : attrInfo) {
                    if (attr.isReadable() && attr.getType().equals("double")) {
                        BASE_KAFKA_METRICS_GAUGE.labels(attr.getName(), Forwarder.getHostname(), RELEASE)
                                .set((Double) MBS.getAttribute(oname, attr.getName()));
                    }
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException | MBeanException | AttributeNotFoundException e) {
            LOGGER.error("", e);
        }
    }
}
