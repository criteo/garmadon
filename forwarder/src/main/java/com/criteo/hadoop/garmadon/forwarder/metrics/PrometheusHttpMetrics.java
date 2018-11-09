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
 */
public class PrometheusHttpMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusHttpMetrics.class);
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    public static final Counter garmadonMetrics = Counter.build()
            .name("garmadon_metrics").help("Garmadon metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static String RELEASE = Optional
            .ofNullable(PrometheusHttpMetrics.class.getPackage().getImplementationVersion())
            .orElse("1.0-SNAPSHOT")
            .replace(".", "_");

    public static Counter.Child eventsReceived = garmadonMetrics.labels("garmadon_events_received",
            Forwarder.hostname,
            RELEASE);
    public static Counter.Child eventsInError = garmadonMetrics.labels("garmadon_events_in_error",
            Forwarder.hostname,
            RELEASE);
    public static Counter.Child greetingsReceived = garmadonMetrics.labels("garmadon_greetings_received",
            Forwarder.hostname,
            RELEASE);
    public static Counter.Child greetingsInError = garmadonMetrics.labels("garmadon_greetings_in_error",
            Forwarder.hostname,
            RELEASE);
    public static Counter.Child forJoin = garmadonMetrics.labels("garmadon_for_join",
            Forwarder.hostname,
            RELEASE);

    private static final Summary baseTypeSizeSummary = Summary.build()
            .name("garmadon_event_size").help("Event sizes in bytes")
            .labelNames("eventType", "hostname", "release")
            .quantile(0.75, 0.01)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    private static final Gauge baseKafkaMetricsGauge = Gauge.build()
            .name("garmadon_kafka_metrics").help("Kafka producer metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static HTTPServer server;

    private static ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    private static ObjectName oname;

    static {
        // Expose JMX, GCs, classloading, thread count, memory pool
        DefaultExports.initialize();
        forJoin.inc();
        try {
            oname = new ObjectName("kafka.producer:type=producer-metrics,client-id=" + Forwarder.PRODUCER_PREFIX_NAME + "." + Forwarder.hostname);
        } catch (MalformedObjectNameException e) {
            LOGGER.error("", e);
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                exposeKafkaMetrics();
            } catch (IntrospectionException | InstanceNotFoundException | ReflectionException | AttributeNotFoundException | MBeanException e) {
                LOGGER.error("", e);
            }
        }, 30, 60, TimeUnit.SECONDS);
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
        return baseTypeSizeSummary.labels(GarmadonSerialization.getTypeName(type), Forwarder.hostname, RELEASE);
    }

    public static void exposeKafkaMetrics() throws IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        if (oname != null) {
            MBeanInfo info = MBS.getMBeanInfo(oname);
            MBeanAttributeInfo[] attrInfo = info.getAttributes();
            for (MBeanAttributeInfo attr : attrInfo) {
                if (attr.isReadable() && attr.getType().equals("double")) {
                    baseKafkaMetricsGauge.labels(attr.getName(), Forwarder.hostname, RELEASE)
                            .set((Double) MBS.getAttribute(oname, attr.getName()));
                }
            }
        }
    }
}
