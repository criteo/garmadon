package com.criteo.hadoop.garmadon.reader.metrics;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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

    public static final Gauge GARMADON_READER_LAST_EVENT_TIMESTAMP = Gauge.build()
        .name("garmadon_reader_last_event_timestamp").help("Garmadon reader last event timestamp")
        .labelNames("name", "hostname", "release", "partition")
        .register();

    public static final Counter.Child ISSUE_READING_PROTO_HEAD = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("issue_reading_proto_head",
        GarmadonReader.getHostname(),
        PrometheusHttpConsumerMetrics.RELEASE);

    public static final Counter.Child ISSUE_READING_PROTO_BODY = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("issue_reading_proto_body",
        GarmadonReader.getHostname(),
        PrometheusHttpConsumerMetrics.RELEASE);


    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusHttpConsumerMetrics.class);
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    // basic kafka consumer jmx metrics
    private static final Gauge BASE_KAFKA_METRICS_GAUGE = Gauge.build()
        .name("garmadon_kafka_metrics").help("Kafka consumer metrics")
        .labelNames("name", "hostname", "release", "consumer_id")
        .register();

    // kafka consumer coordinator metrics
    private static final Gauge KAFKA_COORDINATOR_METRICS_GAUGE = Gauge.build()
        .name("garmadon_kafka_consumer_coordinator_metrics").help("Kafka consumer coordinator metrics")
        .labelNames("name", "hostname", "release", "consumer_id")
        .register();

    // kafka fetch manager global metrics
    private static final Gauge KAFKA_FETCH_MANAGER_METRICS_GAUGE = Gauge.build()
            .name("garmadon_kafka_fetch_manager_metrics").help("Kafka fetch manager metrics")
            .labelNames("name", "hostname", "release", "consumer_id")
            .register();

    // kafka fetch manager metrics per topic and partition
    private static final Gauge KAFKA_FETCH_MANAGER_PER_TOPIC_PARTITION_METRICS_GAUGE = Gauge.build()
            .name("garmadon_kafka_fetch_manager_topic_partition_metrics").help("Kafka fetch manager metrics per topic partition")
            .labelNames("name", "hostname", "release", "consumer_id", "topic", "partition")
            .register();


    private static ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "prometheus-refresh-jmx-kafka-metrics");
            t.setDaemon(true);
            return t;
        }
    });

    // Key => JMX ObjectName
    // Value => Function(ObjectName, Attr) => Gauge.labels
    // then we only have to set the value for this metric
    private static Map<ObjectName, BiFunction<ObjectName, MBeanAttributeInfo, Gauge.Child>> kafkaJmxNameToPrometheusLabeler;

    // specific callback for consumer coordinator metrics (that includes topic and partition labels)
    private static final BiFunction<ObjectName, MBeanAttributeInfo, Gauge.Child> KAFKA_CONSUMER_COORDINATOR_METRICS = (name, attr) -> {
        String clientId = name.getKeyProperty("client-id");
        String topic = name.getKeyProperty("topic");
        String partition = name.getKeyProperty("partition");
        return KAFKA_FETCH_MANAGER_PER_TOPIC_PARTITION_METRICS_GAUGE
                .labels(attr.getName(), GarmadonReader.getHostname(), RELEASE, clientId, topic, partition);
    };

    static {
        // Expose JMX, GCs, classloading, thread count, memory pool
        DefaultExports.initialize();
        kafkaJmxNameToPrometheusLabeler = new HashMap<>();
        try {
            kafkaJmxNameToPrometheusLabeler.put(
                    new ObjectName("kafka.consumer:type=consumer-metrics,client-id=*"),
                    getKafkaMetricConsumerFromGauge(BASE_KAFKA_METRICS_GAUGE));
            kafkaJmxNameToPrometheusLabeler.put(
                    new ObjectName("kafka.consumer:type=consumer-coordinator-metrics,client-id=*"),
                    getKafkaMetricConsumerFromGauge(KAFKA_COORDINATOR_METRICS_GAUGE));
            kafkaJmxNameToPrometheusLabeler.put(
                    new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*"),
                    getKafkaMetricConsumerFromGauge(KAFKA_FETCH_MANAGER_METRICS_GAUGE));
            kafkaJmxNameToPrometheusLabeler.put(
                    new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,topic=garmadon,partition=*"),
                    KAFKA_CONSUMER_COORDINATOR_METRICS);
        } catch (MalformedObjectNameException e) {
            LOGGER.error("", e);
        }

        scheduler.scheduleAtFixedRate(() -> {
            exposeKafkaMetrics();
        }, 0, 30, TimeUnit.SECONDS);
    }

    public static class HTTPServerWithStatus extends HTTPServer {

        public HTTPServerWithStatus(int port) throws IOException {
            super(port);
            server.removeContext("/");
            server.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange t) throws IOException {
                    String response = "<a href=\"/status\"><h1>status</h1></a>" +
                            "<a href=\"/metrics\"><h1>metrics</h1></a>";
                    t.getResponseHeaders().set("Content-Type", "text/plain");
                    t.getResponseHeaders().set("Content-Length", Integer.toString(response.length()));
                    t.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());
                    t.getResponseBody().write(response.getBytes());
                    t.close();
                }
            });

            server.createContext("/status", new HttpHandler() {
                @Override
                public void handle(HttpExchange t) throws IOException {
                    String response = "OK";
                    t.getResponseHeaders().set("Content-Type", "text/plain");
                    t.getResponseHeaders().set("Content-Length", "2");
                    t.sendResponseHeaders(HttpURLConnection.HTTP_OK, 2L);
                    t.getResponseBody().write(response.getBytes());
                    t.close();
                }
            });
        }
    }

    private HTTPServer server;

    public PrometheusHttpConsumerMetrics(int prometheusPort) {
        try {
            server = new HTTPServerWithStatus(prometheusPort);
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

    // util method to build a generic prometheus labeler callback from a gauge
    private static BiFunction<ObjectName, MBeanAttributeInfo, Gauge.Child> getKafkaMetricConsumerFromGauge(Gauge gauge) {
        return (name, attr) -> {
            String clientId = name.getKeyProperty("client-id");
            return gauge.labels(attr.getName(), GarmadonReader.getHostname(), RELEASE, clientId);
        };
    }

    protected static void exposeKafkaMetrics() {
        try {
            for (Map.Entry<ObjectName, BiFunction<ObjectName, MBeanAttributeInfo, Gauge.Child>> entry : kafkaJmxNameToPrometheusLabeler.entrySet()) {
                // jmx object to expose
                ObjectName baseKafkaJmxName = entry.getKey();
                // callback to build a prometheus metric with labels
                BiFunction<ObjectName, MBeanAttributeInfo, Gauge.Child> prometheusMetricLabelsFromObjectNameAndAttr = entry.getValue();

                for (ObjectName name : MBS.queryNames(baseKafkaJmxName, null)) {
                    MBeanInfo info = MBS.getMBeanInfo(name);
                    MBeanAttributeInfo[] attrInfo = info.getAttributes();
                    for (MBeanAttributeInfo attr : attrInfo) {
                        if (attr.isReadable() && attr.getType().equals("double")) {
                            // create metrics target labels and set the corresponding value
                            prometheusMetricLabelsFromObjectNameAndAttr.apply(name, attr)
                                    .set((Double) MBS.getAttribute(name, attr.getName()));
                        }
                    }
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException | MBeanException | AttributeNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
