package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.criteo.hadoop.garmadon.forwarder.Forwarder;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 */
public class PrometheusHttpMetrics {

    public static final Counter garmadonMetrics = Counter.build()
            .name("garmadon_metrics").help("Garmadon metrics")
            .labelNames("name", "hostname", "release")
            .register();

    private static String RELEASE = Optional
            .ofNullable(PrometheusHttpMetrics.class.getPackage().getImplementationVersion()).orElse("1.0-SNAPSHOT");

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

    private static HTTPServer server;

    static {
        // Expose JMX, GCs, classloading, thread count, memory pool
        DefaultExports.initialize();
        forJoin.inc();
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
    }

    public static Summary.Child eventSize(int type) {
        return baseTypeSizeSummary.labels(GarmadonSerialization.getTypeName(type), Forwarder.hostname, RELEASE);
    }
}
