package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.codahale.metrics.*;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.MetricEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MetricsReporter extends ScheduledReporter {

    private static final Logger logger = LoggerFactory.getLogger(MetricsReporter.class);

    private final Consumer<MetricEvent> eventHandler;

    private MetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
                            TimeUnit durationUnit, Consumer<MetricEvent> eventHandler) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.eventHandler = eventHandler;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        MetricEvent metricEvent = new MetricEvent(System.currentTimeMillis());

        if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                addMetric(metricEvent, entry.getKey(), entry.getValue().getCount());
            }
        }

        // Send metric event
        eventHandler.accept(metricEvent);
    }

    private void addMetric(MetricEvent metricEvent, String key, long value) {
        DataAccessEventProtos.Metric metric = DataAccessEventProtos.Metric.newBuilder().setName(key)
                .setValue(value).build();
        metricEvent.addMetric(metric);
    }

    public static MetricsReporter.Builder forRegistry(MetricRegistry registry) {
        return new MetricsReporter.Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Consumer<MetricEvent> eventHandler;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public MetricsReporter.Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public MetricsReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public MetricsReporter.Builder handleEvent(Consumer<MetricEvent> eventHandler){
            this.eventHandler = eventHandler;
            return this;
        }

        /**
         * Builds a {@link ConsoleReporter} with the given properties.
         *
         * @return a {@link ConsoleReporter}
         */
        public MetricsReporter build() throws UnknownHostException {
            return new MetricsReporter(registry,
                    "name",
                    filter,
                    rateUnit,
                    durationUnit,
                    eventHandler);
        }
    }

}
