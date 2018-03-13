package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 */
public class MetricsFactory {

    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static MetricsReporter metricsReporter;

    public final static Counter eventsReceived = metricRegistry.counter("events-received");
    public final static Counter eventsInError = metricRegistry.counter("events-in-error");
    public final static Counter greetingsReceived = metricRegistry.counter("greetings-received");
    public final static Counter greetingsInError = metricRegistry.counter("greetings-in-error");

    public static void startReport(ForwarderEventSender forwarderEventSender) throws UnknownHostException {
         metricsReporter = MetricsReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .handleEvent(forwarderEventSender::sendAsync)
                .build();
        metricsReporter.start(10, TimeUnit.SECONDS);
    }

    public static void stopReport() {
        if(metricsReporter != null) {
            metricsReporter.stop();
        }
    }

}
