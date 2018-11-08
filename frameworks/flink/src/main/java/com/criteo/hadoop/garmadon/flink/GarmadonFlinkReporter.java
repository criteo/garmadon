package com.criteo.hadoop.garmadon.flink;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

/**
 * Metric Reporter for Garmadon.
 *
 * <p>Variables in metrics scope will be sent as tags.
 */
public class GarmadonFlinkReporter implements MetricReporter, Scheduled {
    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {
        System.out.println("notifyOfAddedMetric");
        System.out.println("s: " + s);

        System.out.println("metricGroup.getMetricIdentifier: " + metricGroup.getMetricIdentifier(s));

        metricGroup.getAllVariables().forEach((k, v) -> {
            System.out.println("k: " + k + " ,v:" + v);
        });

        for (String scopeComponent : metricGroup.getScopeComponents()) {
            System.out.println("getScopeComponents: " + scopeComponent);
        }

        if (metric instanceof Counter) {
            System.out.println("Counter");
            Counter c = (Counter) metric;
            System.out.println("metric: " + c.getCount());
        } else if (metric instanceof Gauge) {
            System.out.println("Gauge");
            Gauge g = (Gauge) metric;
            System.out.println("metric: " + g.getValue());
        } else if (metric instanceof Meter) {
            System.out.println("Meter");
            Meter m = (Meter) metric;
            System.out.println("metric: " + m.getCount() + "-" + m.getRate());
        } else if (metric instanceof Histogram) {
            System.out.println("Histogram");
            Histogram h = (Histogram) metric;
            System.out.println("metric: " + h.getCount() + "-" + h.getStatistics().getValues());
        } else {
            System.out.println("Other");
            System.out.println("metric: " + metric.getClass().getName());
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
        System.out.println("notifyOfRemovedMetric, metric: " + metric.toString() + ", s: " + ", metricGroup: " + metricGroup.toString());
    }

    @Override
    public void report() {
        System.out.println("report");
    }
}
