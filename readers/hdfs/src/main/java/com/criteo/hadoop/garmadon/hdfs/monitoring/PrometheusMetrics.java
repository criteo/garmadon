package com.criteo.hadoop.garmadon.hdfs.monitoring;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.SimpleCollector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PrometheusMetrics {
    // Output metrics
    private static final Counter TMP_FILES_OPENED = buildCounter("tmp_files_opened",
        "Success opening temporary files", false);
    private static final Counter TMP_FILE_OPEN_FAILURES = buildCounter("tmp_file_open_failures",
        "Failures opening temporary files", false);
    private static final Counter FILES_COMMITTED = buildCounter("files_committed",
        "Files moved to their final destination", false);
    private static final Counter MESSAGES_WRITTEN = buildCounter("messages_written",
        "Messages written to temporary files");
    private static final Counter MESSAGES_WRITING_FAILURES = buildCounter("messages_writing_failures",
        "Errors when writing messages to temporary files");
    private static final Counter HEARTBEATS_SENT = buildCounter("heartbeats_sent",
        "Number of heartbeats sent");
    private static final Counter CHECKPOINTS_FAILURES = buildCounter("checkpoints_failures",
        "Number of checkpointing failures");
    private static final Counter CHECKPOINTS_SUCCESSES = buildCounter("checkpoints_successes",
        "Number of checkpointing successes");

    // Input metrics
    private static final Gauge CURRENT_RUNNING_OFFSETS = buildGauge("current_running_offsets",
        "Current offsets");
    private static final Gauge LATEST_COMMITTED_OFFSETS = buildGauge("latest_committed_offsets",
        "Latest committed offsets");
    private static final Gauge LATEST_COMMITTED_TIMESTAMPS = buildGauge("latest_committed_timestamps",
        "Latest committed timestamps");

    //We must remember what child collectors are assigned to what partition
    //and to be able to remove them, we must remember the parent collector and the labels associated to the child collector
    private static final Map<Integer, Map<SimpleCollector<?>, Set<List<String>>>> REGISTERED_COLLECTORS = new ConcurrentHashMap<>();

    private PrometheusMetrics() {
    }

    public static Counter.Child tmpFileOpenFailuresCounter(String eventName) {
        return buildChild(TMP_FILE_OPEN_FAILURES, eventName);
    }

    public static Counter.Child tmpFilesOpened(String eventName) {
        return buildChild(TMP_FILES_OPENED, eventName);
    }

    public static Counter.Child filesCommittedCounter(String eventName) {
        return buildChild(FILES_COMMITTED, eventName);
    }

    public static Counter.Child checkpointFailuresCounter(String eventName, int partition) {
        return buildChild(CHECKPOINTS_FAILURES, partition, eventName, String.valueOf(partition));
    }

    public static Counter.Child checkpointSuccessesCounter(String eventName, int partition) {
        return buildChild(CHECKPOINTS_SUCCESSES, partition, eventName, String.valueOf(partition));
    }

    public static Counter.Child messageWritingFailuresCounter(String eventName, int partition) {
        return buildChild(MESSAGES_WRITING_FAILURES, partition, eventName, String.valueOf(partition));
    }

    public static Counter.Child messageWrittenCounter(String eventName, int partition) {
        return buildChild(MESSAGES_WRITTEN, partition, eventName, String.valueOf(partition));
    }

    public static Counter.Child hearbeatsSentCounter(String eventName, int partition) {
        return buildChild(HEARTBEATS_SENT, partition, eventName, String.valueOf(partition));
    }

    public static Gauge.Child currentRunningOffsetsGauge(String eventName, int partition) {
        return buildChild(CURRENT_RUNNING_OFFSETS, partition, eventName, String.valueOf(partition));
    }

    public static Gauge.Child latestCommittedOffsetGauge(String eventName, int partition) {
        return buildChild(LATEST_COMMITTED_OFFSETS, partition, eventName, String.valueOf(partition));
    }

    public static Gauge.Child latestCommittedTimestampGauge(String eventName, int partition) {
        Gauge.Child gauge = buildChild(LATEST_COMMITTED_TIMESTAMPS, partition, eventName, String.valueOf(partition));
        return new Gauge.Child() {
            @Override
            public void set(double val) {
                gauge.set(Double.max(gauge.get(), val));
            }

            @Override
            public double get() {
                return gauge.get();
            }
        };
    }

    private static <CHILD> CHILD buildChild(SimpleCollector<CHILD> collector, String... labels) {
        return collector.labels(mergeWithDefault(labels));
    }

    private static <CHILD> CHILD buildChild(SimpleCollector<CHILD> collector, Integer partition, String... labels) {
        registerPartitionCollector(collector, partition, labels);
        return buildChild(collector, labels);
    }

    private static Counter buildCounter(String name, String help) {
        return buildCounter(name, help, true);
    }

    private static Counter buildCounter(String name, String help, boolean withPartition) {
        Counter.Builder builder = Counter.build()
            .name(name).help(help);

        if (withPartition) {
            builder.labelNames("name", "partition", "hostname", "release");
        } else {
            builder.labelNames("name", "hostname", "release");
        }

        return builder.register();
    }

    private static Gauge buildGauge(String name, String help) {
        return Gauge.build()
            .name(name).help(help)
            .labelNames("name", "partition", "hostname", "release")
            .register();
    }

    public static void clearCollectors() {
        REGISTERED_COLLECTORS.forEach((partition, parentCollectorToChildLabels) -> {
            parentCollectorToChildLabels.forEach((collector, labels) -> collector.clear());
        });
        REGISTERED_COLLECTORS.clear();
    }

    public static void clearPartitionCollectors(int partition) {
        REGISTERED_COLLECTORS
            .get(partition)
            .forEach((collector, childLabels) -> childLabels.forEach(labels -> collector.remove(labels.toArray(new String[0]))));
    }

    public static Map<Integer, Map<SimpleCollector<?>, Set<List<String>>>> getRegisteredCollectors() {
        return Collections.unmodifiableMap(REGISTERED_COLLECTORS);
    }

    private static <CHILD> void registerPartitionCollector(SimpleCollector<CHILD> collector, Integer partition, String... labels) {
        REGISTERED_COLLECTORS.computeIfAbsent(partition, ignored -> new HashMap<>());
        REGISTERED_COLLECTORS.computeIfPresent(partition, (part, parentCollectorToChildLabels) -> {
            parentCollectorToChildLabels.computeIfAbsent(collector, ignored -> new HashSet<>());
            parentCollectorToChildLabels.computeIfPresent(collector, (ignored, childLabels) -> {
                childLabels.add(Arrays.asList(mergeWithDefault(labels)));
                return childLabels;
            });
            return parentCollectorToChildLabels;
        });
    }

    private static String[] mergeWithDefault(String... labels) {
        String[] allLabels = new String[labels.length + 2];
        System.arraycopy(labels, 0, allLabels, 0, labels.length);
        allLabels[labels.length] = GarmadonReader.getHostname();
        allLabels[labels.length + 1] = PrometheusHttpConsumerMetrics.RELEASE;
        return allLabels;
    }
}
