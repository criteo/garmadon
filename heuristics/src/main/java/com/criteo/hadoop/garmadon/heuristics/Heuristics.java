package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.events.StateEvent;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.criteo.jvm.JVMStatisticsProtos;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasTag;
import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasType;

public class Heuristics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);

    private final GarmadonReader reader;
    private final List<GCStatsHeuristic> gcStatsHeuristics = new CopyOnWriteArrayList<>();
    private final List<JVMStatsHeuristic> jvmStatsHeuristics = new CopyOnWriteArrayList<>();
    private final Map<String, Set<String>> containersPerApp = new HashMap<>();

    private final FileHeuristic fileHeuristic;

    public Heuristics(String kafkaConnectString, String kafkaGroupId, HeuristicsResultDB db) {
        this.fileHeuristic = new FileHeuristic(db);

        this.reader = GarmadonReader.Builder
                .stream(kafkaConnectString)
                .withGroupId(kafkaGroupId)
                .withKafkaProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .withKafkaProp(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.GC_EVENT)), this::processGcEvent)
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.JVMSTATS_EVENT)), this::processJvmStatEvent)
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.STATE_EVENT)), this::processStateEvent)
                .intercept(
                        hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FS_EVENT)),
                        msg -> fileHeuristic.compute(msg.getHeader().getApplicationId(), msg.getHeader().getAppAttemptID(),
                                msg.getHeader().getContainerId(), (DataAccessEventProtos.FsEvent) msg.getBody())
                )
                .intercept(
                        hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.STATE_EVENT)),
                        msg -> fileHeuristic.compute(msg.getHeader().getApplicationId(), msg.getHeader().getAppAttemptID(),
                                msg.getHeader().getContainerId(), (DataAccessEventProtos.StateEvent) msg.getBody())
                )
                .build();

        gcStatsHeuristics.add(new GCCause(db));
        gcStatsHeuristics.add(new G1GC(db));
        gcStatsHeuristics.add(new HeapUsage(db));

        jvmStatsHeuristics.add(new Threads(db));
        jvmStatsHeuristics.add(new CodeCacheUsage(db));
        jvmStatsHeuristics.add(new Safepoints(db));
        jvmStatsHeuristics.add(new Locks(db));
    }

    public void start() {
        reader.startReading().whenComplete(this::completeReading);
    }

    public void stop() {
        reader.stopReading();
    }

    private void completeReading(Void dummy, Throwable ex) {
        if (ex != null) {
            LOGGER.error("Reading was stopped due to exception");
            ex.printStackTrace();
        } else {
            LOGGER.info("Done reading !");
        }
    }

    private void processGcEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String attemptId = msg.getHeader().getAppAttemptID();
        String containerId = msg.getHeader().getContainerId();
        Set<String> containers = containersPerApp.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId)
                , s -> new HashSet<>());
        containers.add(containerId);
        JVMStatisticsProtos.GCStatisticsData gcStats = (JVMStatisticsProtos.GCStatisticsData) msg.getBody();
        gcStatsHeuristics.forEach(h -> h.process(applicationId, attemptId, containerId, gcStats));
    }

    private void processJvmStatEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String attemptId = msg.getHeader().getAppAttemptID();
        String containerId = msg.getHeader().getContainerId();
        Set<String> containers = containersPerApp.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId)
                , s -> new HashSet<>());
        containers.add(containerId);
        JVMStatisticsProtos.JVMStatisticsData jvmStats = (JVMStatisticsProtos.JVMStatisticsData) msg.getBody();
        jvmStatsHeuristics.forEach(h -> h.process(applicationId, attemptId, containerId, jvmStats));
    }

    // TODO handle safety net for APP_END_EVENT => compute the normal interval based on timestamp
    // TODO => if no msg for an appId after 2/3*interval (still based on timestamp) => trigger APP_END
    private void processStateEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String attemptId = msg.getHeader().getAppAttemptID();
        String containerId = msg.getHeader().getContainerId();
        DataAccessEventProtos.StateEvent stateEvent = (DataAccessEventProtos.StateEvent) msg.getBody();
        if (StateEvent.State.END.toString().equals(stateEvent.getState())) {
            gcStatsHeuristics.forEach(h -> h.onContainerCompleted(applicationId, attemptId, containerId));
            jvmStatsHeuristics.forEach(h -> h.onContainerCompleted(applicationId, attemptId, containerId));
            Set<String> appContainers = containersPerApp.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId), s -> new HashSet<>());
            if (appContainers.size() == 0)
                return;
            appContainers.remove(containerId);
            if (appContainers.size() == 0) {
                LOGGER.info("App {} is finished. All containers have been removed", applicationId);
                containersPerApp.remove(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
                gcStatsHeuristics.forEach(h -> h.onAppCompleted(applicationId, attemptId));
                jvmStatsHeuristics.forEach(h -> h.onAppCompleted(applicationId, attemptId));
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            printHelp();
            return;
        }
        String kafkaConnectString = args[0];
        String kafkaGroupId = args[1];
        String dbConnectionString = args[2];
        String dbUser = args[3];
        String dbPassword = args[4];
        HeuristicsResultDB db = new HeuristicsResultDB(dbConnectionString, dbUser, dbPassword);
        Heuristics heuristics = new Heuristics(kafkaConnectString, kafkaGroupId, db);
        heuristics.start();
        Runtime.getRuntime().addShutdownHook(new Thread(heuristics::stop));
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.heuristics.Heuristics <kafkaConnectionString> <kafkaGroupId> <DrElephantDBConnectionString> <DrElephantDBUser> <DrElephantDBPassword>");
    }

}
