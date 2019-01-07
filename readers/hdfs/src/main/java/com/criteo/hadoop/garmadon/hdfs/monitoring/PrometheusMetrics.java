package com.criteo.hadoop.garmadon.hdfs.monitoring;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public final class PrometheusMetrics {
    // Output metrics
    public static final Counter TMP_FILES_OPENED = buildCounter("tmp_files_opened",
            "Success opening temporary files", false);
    public static final Counter TMP_FILE_OPEN_FAILURES = buildCounter("tmp_file_open_failures",
            "Failures opening temporary files", false);
    public static final Counter FILES_COMMITTED = buildCounter("files_committed",
            "Files moved to their final destination", false);
    public static final Counter FILE_COMMIT_FAILURES = buildCounter("file_commit_failures",
            "Failures when moving files to their final destination", false);
    public static final Counter MESSAGES_WRITTEN = buildCounter("messages_written",
            "Messages written to temporary files");
    public static final Counter MESSAGES_WRITING_FAILURES = buildCounter("messages_writing_failures",
            "Errors when writing messages to temporary files");
    public static final Counter HEARTBEATS_SENT = buildCounter("heartbeats_sent",
            "Number of heartbeats sent");

    // Input metrics
    public static final Gauge CURRENT_RUNNING_OFFSETS = buildGauge("current_running_offsets",
            "Current offsets");
    public static final Gauge LATEST_COMMITTED_OFFSETS = buildGauge("latest_committed_offsets",
            "Latest committed offsets");

    private PrometheusMetrics() {
    }

    public static Gauge.Child buildGaugeChild(Gauge baseGauge, String eventName, int partition) {
        return baseGauge.labels(eventName, GarmadonReader.getHostname(), PrometheusHttpConsumerMetrics.RELEASE,
                String.valueOf(partition));
    }

    public static Counter.Child buildCounterChild(Counter baseCounter, String eventName, int partition) {
        return baseCounter.labels(eventName, GarmadonReader.getHostname(), PrometheusHttpConsumerMetrics.RELEASE,
                String.valueOf(partition));
    }

    public static Counter.Child buildCounterChild(Counter baseCounter, String eventName) {
        return baseCounter.labels(eventName, GarmadonReader.getHostname(), PrometheusHttpConsumerMetrics.RELEASE);
    }

    private static Counter buildCounter(String name, String help) {
        return buildCounter(name, help, true);
    }

    private static Counter buildCounter(String name, String help, boolean withPartition) {
        Counter.Builder builder = Counter.build()
                .name(name).help(help);

        if (withPartition) builder.labelNames("name", "hostname", "release", "partition");
        else builder.labelNames("name", "hostname", "release");

        return builder.register();
    }

    private static Gauge buildGauge(String name, String help) {
        return Gauge.build()
                .name(name).help(help)
                .labelNames("name", "hostname", "release", "partition")
                .register();
    }
}
