package com.criteo.hadoop.garmadon.reader.metrics;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
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
public class PrometheusHttpConsumerMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusHttpConsumerMetrics.class);
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    public static String RELEASE = Optional
            .ofNullable(PrometheusHttpConsumerMetrics.class.getPackage().getImplementationVersion())
            .orElse("1.0-SNAPSHOT")
            .replace(".", "_");

    public static final Counter garmadonReaderMetrics = Counter.build()
            .name("garmadon_reader_metrics").help("Garmadon reader metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static final Gauge baseKafkaMetricsGauge = Gauge.build()
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
            try {
                exposeKafkaMetrics();
            } catch (IntrospectionException | InstanceNotFoundException | ReflectionException | AttributeNotFoundException | MBeanException e) {
                LOGGER.warn("", e);
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    protected static void exposeKafkaMetrics() throws IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        if (oName != null) {
            MBeanInfo info = MBS.getMBeanInfo(oName);
            MBeanAttributeInfo[] attrInfo = info.getAttributes();
            for (MBeanAttributeInfo attr : attrInfo) {
                if (attr.isReadable() && attr.getType().equals("double")) {
                    baseKafkaMetricsGauge.labels(attr.getName(), GarmadonReader.HOSTNAME, RELEASE)
                            .set((Double) MBS.getAttribute(oName, attr.getName()));
                }
            }
        }
    }

    public static Counter.Child issueReadingGarmadonMessageBadHead = PrometheusHttpConsumerMetrics.garmadonReaderMetrics.labels("issue_reading_garmadon_message_bad_head",
            GarmadonReader.HOSTNAME,
            PrometheusHttpConsumerMetrics.RELEASE);

    public static Counter.Child issueReadingProtoHead = PrometheusHttpConsumerMetrics.garmadonReaderMetrics.labels("issue_reading_proto_head",
            GarmadonReader.HOSTNAME,
            PrometheusHttpConsumerMetrics.RELEASE);

    public static Counter.Child issueReadingProtoBody = PrometheusHttpConsumerMetrics.garmadonReaderMetrics.labels("issue_reading_proto_body",
            GarmadonReader.HOSTNAME,
            PrometheusHttpConsumerMetrics.RELEASE);

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
}
