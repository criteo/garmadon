package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.criteo.hadoop.garmadon.forwarder.Forwarder;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 */
public class PrometheusHttpMetrics {
    public static final Counter garmadonMetrics = Counter.build()
            .name("garmadon_metrics").help("Garmadon metrics")
            .labelNames("name", "instance", "release")
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

}
