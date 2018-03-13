package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.StateEvent;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.criteo.jvm.JVMStatisticsProtos;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class Extractor {
    private static final GarmadonMessageFilter JVM_FILTER = new JVMFilter();

    private final GarmadonReader reader;
    private String containerId;
    private Map<String, Stats> containers = new HashMap<>();

    public Extractor(String kafkaConnectString) {
        GarmadonReader.Builder builder = GarmadonReader.Builder.withKafkaConnectString(kafkaConnectString);
        reader = builder
                .listeningToMessages(JVM_FILTER, this::processStats)
                .build();
    }

    public Extractor(String kafkaConnectString, String containerId) {
        GarmadonReader.Builder builder = GarmadonReader.Builder.withKafkaConnectString(kafkaConnectString);
        reader = builder
                .listeningToMessages(JVM_FILTER, this::process)
                .build();
        this.containerId = containerId;
    }

    public void start() {
        reader.startReading().whenComplete(this::completeReading);
    }

    private void processStats(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String containerId = msg.getHeader().getContainerId();
        Stats stats = containers.computeIfAbsent(containerId, s -> new Stats(applicationId, containerId));
        switch (msg.getType()) {
            case GarmadonSerialization.TypeMarker.GC_EVENT: {
                stats.gcStatCount++;
                break;
            }
            case GarmadonSerialization.TypeMarker.JVMSTATS_EVENT: {
                if (msg.getHeader().getTag().equals("FORWARDER")) {

                } else {
                    stats.jvmStatCount++;
                }
                break;
            }
            case GarmadonSerialization.TypeMarker.STATE_EVENT: {
                System.out.println(stats);
                break;
            }
        }

    }
    private void process(GarmadonMessage msg) {
        if (!msg.getHeader().getContainerId().equals(containerId))
            return;
        switch (msg.getType()) {
            case GarmadonSerialization.TypeMarker.GC_EVENT: {
                JVMStatisticsProtos.GCStatisticsData gcStats = (JVMStatisticsProtos.GCStatisticsData) msg.getBody();
                StringBuilder sb = new StringBuilder();
                String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(gcStats.getTimestamp()));
                sb.append(timestamp).append(" ");
                sb.append(gcStats.getCollectorName()).append(" occurred, took ").append(gcStats.getPauseTime()).append("ms");
                sb.append(" (").append(gcStats.getCause()).append(") ");
                appendSpaceInfo(sb, "eden", gcStats.getEdenBefore(), gcStats.getEdenAfter());
                appendSpaceInfo(sb, "survivor", gcStats.getSurvivorBefore(), gcStats.getSurvivorAfter());
                appendSpaceInfo(sb, "old", gcStats.getOldBefore(), gcStats.getOldAfter());
                appendSpaceInfo(sb, "metaspace", gcStats.getMetaspaceBefore(), gcStats.getMetaspaceAfter());
                appendSpaceInfo(sb, "code", gcStats.getCodeBefore(), gcStats.getCodeAfter());
                System.out.println(sb.toString());
                break;
            }
            case GarmadonSerialization.TypeMarker.JVMSTATS_EVENT: {
                JVMStatisticsProtos.JVMStatisticsData jvmStats = (JVMStatisticsProtos.JVMStatisticsData) msg.getBody();
                StringBuilder sb = new StringBuilder();
                String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(jvmStats.getTimestamp()));
                sb.append(timestamp).append(" ");
                for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
                    sb.append(section.getName()).append("[");
                    for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                        sb.append(property.getName()).append("=").append(property.getValue()).append(" ");
                    }
                    sb.append("] ");
                }
                System.out.println(sb.toString());
                break;
            }
            case GarmadonSerialization.TypeMarker.STATE_EVENT:
                DataAccessEventProtos.StateEvent stateEvent = (DataAccessEventProtos.StateEvent) msg.getBody();
                if (StateEvent.State.END.toString().equals(stateEvent.getState())) {
                    System.exit(0);
                    //System.out.println("END!");
                }
                break;
        }
    }

    private static void appendSpaceInfo(StringBuilder sb, String spaceName, long before, long after) {
        long delta = before - after;
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
        if ("stats".equals(containerId))
            extractor = new Extractor(kafkaConnectString);
        else
            extractor = new Extractor(kafkaConnectString, containerId);
        extractor.start();
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.reader.Extractor <kafkaConnectionString> <containerId>");
    }

    private static class JVMFilter implements GarmadonMessageFilter {

        @Override
        public boolean acceptsType(int type) {
            return type == GarmadonSerialization.TypeMarker.GC_EVENT
                    || type == GarmadonSerialization.TypeMarker.JVMSTATS_EVENT
                    || type == GarmadonSerialization.TypeMarker.STATE_EVENT;
        }

        @Override
        public boolean acceptsHeader(DataAccessEventProtos.Header header) {
            return true;
        }
    }

    private static class Stats {
        final String applicationId;
        final String containerId;
        private long jvmStatCount;
        private long gcStatCount;

        Stats(String applicationId, String containerId) {
            this.applicationId = applicationId;
            this.containerId = containerId;
        }

        @Override
        public String toString() {
            return "ApplicationId: " + applicationId + " ContainerId: " + containerId + " JVMStats: " + jvmStatCount + " GCStats: " + gcStatCount;
        }
    }
}
