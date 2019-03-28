package com.criteo.hadoop.garmadon.tool;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.*;

public final class Extractor {

    private final GarmadonReader reader;
    private Map<String, Stats> containers = new HashMap<>();

    private Extractor(String kafkaConnectString) {
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);

        reader = GarmadonReader.Builder
            .stream(new KafkaConsumer<>(props))
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.GC_EVENT)),
                msg -> getStats(msg).gcStatCount++)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.JVMSTATS_EVENT)),
                msg -> getStats(msg).jvmStatCount++)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.STATE_EVENT)),
                msg -> System.out.println(getStats(msg)))
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_MANAGER_EVENT)),
                this::processFlinkJobManagerEvent)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_EVENT)),
                this::processFlinkJobEvent)
            .build();
    }

    private Extractor(String kafkaConnectString, String containerId) {
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);

        reader = GarmadonReader.Builder.stream(new KafkaConsumer<>(props))
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasContainerId(containerId)).and(hasType(GarmadonSerialization.TypeMarker.GC_EVENT)),
                this::processGcEvent)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasContainerId(containerId)).and(hasType(GarmadonSerialization.TypeMarker.JVMSTATS_EVENT)),
                this::processJvmStatEvent)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasContainerId(containerId)).and(hasType(GarmadonSerialization.TypeMarker.STATE_EVENT)),
                this::processStateEvent)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasContainerId(containerId))
                    .and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_MANAGER_EVENT)),
                this::processFlinkJobManagerEvent)
            .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasContainerId(containerId))
                    .and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_EVENT)),
                this::processFlinkJobEvent)
            .build();
    }

    private void start() {
        reader.startReading().whenComplete(this::completeReading);
    }

    private void stop() {
        reader.stopReading().whenComplete(this::completeReading);
    }

    private Stats getStats(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String containerId = msg.getHeader().getContainerId();
        String framework = msg.getHeader().getFramework();
        return containers.computeIfAbsent(containerId, s -> new Stats(applicationId, containerId, framework));
    }

    private void processGcEvent(GarmadonMessage msg) {
        JVMStatisticsEventsProtos.GCStatisticsData gcStats = (JVMStatisticsEventsProtos.GCStatisticsData) msg.getBody();
        StringBuilder sb = new StringBuilder();
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(msg.getTimestamp()));
        sb.append(timestamp).append(" ");
        sb.append(gcStats.getCollectorName()).append(" occurred, took ").append(gcStats.getPauseTime()).append("ms");
        sb.append(" (").append(gcStats.getCause()).append(") ");
        appendSpaceInfo(sb, "eden", gcStats.getEdenBefore(), gcStats.getEdenAfter());
        appendSpaceInfo(sb, "survivor", gcStats.getSurvivorBefore(), gcStats.getSurvivorAfter());
        appendSpaceInfo(sb, "old", gcStats.getOldBefore(), gcStats.getOldAfter());
        appendSpaceInfo(sb, "metaspace", gcStats.getMetaspaceBefore(), gcStats.getMetaspaceAfter());
        appendSpaceInfo(sb, "code", gcStats.getCodeBefore(), gcStats.getCodeAfter());
        System.out.println(sb.toString());
    }

    private void processJvmStatEvent(GarmadonMessage msg) {
        JVMStatisticsEventsProtos.JVMStatisticsData jvmStats = (JVMStatisticsEventsProtos.JVMStatisticsData) msg.getBody();
        StringBuilder sb = new StringBuilder();
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(msg.getTimestamp()));
        sb.append(timestamp).append(" ");
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            sb.append(section.getName()).append("[");
            for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                sb.append(property.getName()).append("=").append(property.getValue()).append(" ");
            }
            sb.append("] ");
        }
        System.out.println(sb.toString());
    }

    private void processStateEvent(GarmadonMessage msg) {
        DataAccessEventProtos.StateEvent stateEvent = (DataAccessEventProtos.StateEvent) msg.getBody();
        if (State.END.toString().equals(stateEvent.getState())) {
            System.exit(0);
        }
    }

    private void processFlinkJobManagerEvent(GarmadonMessage msg) {
        FlinkEventProtos.JobManagerEvent event = (FlinkEventProtos.JobManagerEvent) msg.getBody();
        StringBuilder sb = new StringBuilder();
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(msg.getTimestamp()));
        sb.append(timestamp).append(" ");
        sb.append("Host[").append(msg.getHeader().getHostname()).append("] ");
        for (FlinkEventProtos.Property property : event.getMetricsList()) {
            sb.append("[").append(property.getName()).append("=").append(property.getValue()).append("] ");
        }
        System.out.println(sb.toString());
    }

    private void processFlinkJobEvent(GarmadonMessage msg) {
        FlinkEventProtos.JobEvent event = (FlinkEventProtos.JobEvent) msg.getBody();
        StringBuilder sb = new StringBuilder();
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(msg.getTimestamp()));
        sb.append(timestamp).append(" ");
        sb.append("Host[").append(msg.getHeader().getHostname()).append("] ");
        sb.append("JobId[").append(event.getJobId()).append("] ");
        sb.append("JobName[").append(event.getJobName()).append("] ");
        for (FlinkEventProtos.Property property : event.getMetricsList()) {
            sb.append("[").append(property.getName()).append("=").append(property.getValue()).append("] ");
        }
        System.out.println(sb.toString());
    }

    private static void appendSpaceInfo(StringBuilder sb, String spaceName, long before, long after) {
        long delta = after - before;
        sb.append(spaceName).append("[").append(delta > 0 ? "+" : "").append(delta).append("](");
        sb.append(before).append("->").append(after).append(") ");
    }

    private void completeReading(Void dummy, Throwable ex) {
        if (ex != null) {
            System.out.println("Reading was stopped due to exception");
            ex.printStackTrace();
        } else {
            System.out.println("Done reading !");
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            printHelp();
            return;
        }
        String kafkaConnectString = args[0];
        String containerId = args[1];
        Extractor extractor;
        if ("stats".equals(containerId)) {
            extractor = new Extractor(kafkaConnectString);
        } else {
            extractor = new Extractor(kafkaConnectString, containerId);
        }
        extractor.start();
        Runtime.getRuntime().addShutdownHook(new Thread(extractor::stop));
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.Extractor <kafkaConnectionString> <containerId>");
    }

    private static class Stats {
        private final String applicationId;
        private final String containerId;
        private String framework;
        private long jvmStatCount;
        private long gcStatCount;

        Stats(String applicationId, String containerId, String framework) {
            this.applicationId = applicationId;
            this.containerId = containerId;
            this.framework = framework;
        }

        @Override
        public String toString() {
            return " Framework: " + framework + "ApplicationId: " + applicationId + " ContainerId: " + containerId
                + " JVMStats: " + jvmStatCount + " GCStats: " + gcStatCount;
        }
    }
}
