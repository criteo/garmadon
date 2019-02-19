package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.heuristics.configurations.HeuristicsConfiguration;
import com.criteo.hadoop.garmadon.heuristics.configurations.HeuristicsReaderConfiguration;
import com.criteo.hadoop.garmadon.heuristics.flink.FlinkCheckpointDuration;
import com.criteo.hadoop.garmadon.heuristics.flink.FlinkHeuristic;
import com.criteo.hadoop.garmadon.heuristics.flink.FlinkHeuristicsManager;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.enums.Framework;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.*;

public class Heuristics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);

    private final GarmadonReader reader;
    private final List<GCStatsHeuristic> gcStatsHeuristics = new CopyOnWriteArrayList<>();
    private final List<JVMStatsHeuristic> jvmStatsHeuristics = new CopyOnWriteArrayList<>();
    private final List<Heuristic> heuristics = new CopyOnWriteArrayList<>();
    private final Map<String, Set<String>> containersPerApp = new HashMap<>();

    private final FileHeuristic fileHeuristic;
    private PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;
    private final FlinkHeuristicsManager flinkHeuristicsManager;

    public Heuristics(Properties kafkaSettings, int prometheusPort, HeuristicsResultDB db, HeuristicsConfiguration heuristicsConfiguration) {
        this.fileHeuristic = new FileHeuristic(db, heuristicsConfiguration.getFileMaxCreatedFiles());

        //setup Prometheus client
        prometheusHttpConsumerMetrics = new PrometheusHttpConsumerMetrics(prometheusPort);
        Properties props = new Properties();
        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);

        props.putAll(kafkaSettings);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        List<FlinkHeuristic> flinkHeuristics = Arrays.asList(new FlinkCheckpointDuration(db));
        flinkHeuristicsManager = new FlinkHeuristicsManager(flinkHeuristics);

        this.reader = GarmadonReader.Builder
                .stream(new KafkaConsumer<>(props))
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.GC_EVENT)
                        .and(hasFramework(Framework.SPARK.name()).or(hasFramework(Framework.MAP_REDUCE.name())))), this::processGcEvent)
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.JVMSTATS_EVENT)
                        .and(hasFramework(Framework.SPARK.name()).or(hasFramework(Framework.MAP_REDUCE.name())))), this::processJvmStatEvent)
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.STATE_EVENT)
                        .and(hasFramework(Framework.SPARK.name()).or(hasFramework(Framework.MAP_REDUCE.name())))), this::processStateEvent)
                .intercept(
                        hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FS_EVENT)
                                .and(hasFramework(Framework.SPARK.name()).or(hasFramework(Framework.MAP_REDUCE.name())))),
                        msg -> fileHeuristic.compute(msg.getHeader().getApplicationId(), msg.getHeader().getAttemptId(),
                                msg.getHeader().getContainerId(), (DataAccessEventProtos.FsEvent) msg.getBody())
                )
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_MANAGER_EVENT)),
                  flinkHeuristicsManager::processFlinkJobManagerEvent)
                .intercept(hasTag(Header.Tag.YARN_APPLICATION).and(hasType(GarmadonSerialization.TypeMarker.FLINK_JOB_EVENT)),
                  flinkHeuristicsManager::processFlinkJobEvent)
                .beforeIntercept(this::registerAppContainer)
                .build();

        gcStatsHeuristics.add(new GCCause(db));
        gcStatsHeuristics.add(new G1GC(db));

        jvmStatsHeuristics.add(new HeapUsage(db));
        jvmStatsHeuristics.add(new Threads(db));
        jvmStatsHeuristics.add(new CodeCacheUsage(db));
        jvmStatsHeuristics.add(new Safepoints(db));
        jvmStatsHeuristics.add(new Locks(db));

        this.heuristics.add(fileHeuristic);
        this.heuristics.addAll(gcStatsHeuristics);
        this.heuristics.addAll(jvmStatsHeuristics);
        this.heuristics.addAll(flinkHeuristicsManager.getHeuristics());
        db.updateHeuristicHelp(heuristics);
    }


    public void start() {
        reader.startReading().whenComplete(this::completeReading);
    }

    public void stop() {
        reader.stopReading().whenComplete((vd, ex) -> prometheusHttpConsumerMetrics.terminate());
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
        String attemptId = msg.getHeader().getAttemptId();
        String containerId = msg.getHeader().getContainerId();
        Long timestamp = msg.getTimestamp();
        JVMStatisticsEventsProtos.GCStatisticsData gcStats = (JVMStatisticsEventsProtos.GCStatisticsData) msg.getBody();
        gcStatsHeuristics.forEach(h -> h.process(timestamp, applicationId, attemptId, containerId, gcStats));
    }

    private void processJvmStatEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String attemptId = msg.getHeader().getAttemptId();
        String containerId = msg.getHeader().getContainerId();
        Long timestamp = msg.getTimestamp();
        JVMStatisticsEventsProtos.JVMStatisticsData jvmStats = (JVMStatisticsEventsProtos.JVMStatisticsData) msg.getBody();
        jvmStatsHeuristics.forEach(h -> h.process(timestamp, applicationId, attemptId, containerId, jvmStats));
    }

    private void registerAppContainer(GarmadonMessage msg) {
        if (msg.getType() != GarmadonSerialization.TypeMarker.STATE_EVENT) {
            String applicationId = msg.getHeader().getApplicationId();
            String attemptId = msg.getHeader().getAttemptId();
            String containerId = msg.getHeader().getContainerId();
            Set<String> containers = containersPerApp.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId), s -> new HashSet<>());
            containers.add(containerId);
        }
    }

    // TODO handle safety net for APP_END_EVENT => compute the normal interval based on timestamp
    // TODO => if no msg for an appId after 2/3*interval (still based on timestamp) => trigger APP_END
    private void processStateEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String attemptId = msg.getHeader().getAttemptId();
        String containerId = msg.getHeader().getContainerId();
        DataAccessEventProtos.StateEvent stateEvent = (DataAccessEventProtos.StateEvent) msg.getBody();
        if (State.END.toString().equals(stateEvent.getState())) {
            heuristics.forEach(h -> h.onContainerCompleted(applicationId, attemptId, containerId));
            Set<String> appContainers = containersPerApp.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId), s -> new HashSet<>());
            if (appContainers.size() == 0) return;
            appContainers.remove(containerId);
            if (appContainers.size() == 0) {
                LOGGER.info("App {} is finished. All containers have been removed", applicationId);
                containersPerApp.remove(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
                heuristics.forEach(h -> h.onAppCompleted(applicationId, attemptId));
                flinkHeuristicsManager.onAppCompleted(applicationId, attemptId);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        HeuristicsReaderConfiguration config = ReaderConfiguration.loadConfig(HeuristicsReaderConfiguration.class);

        HeuristicsResultDB db = new HeuristicsResultDB(config.getDb());
        Heuristics heuristics = new Heuristics(config.getKafka().getSettings(), config.getPrometheus().getPort(), db, config.getHeuristicsConfiguration());
        heuristics.start();
        Runtime.getRuntime().addShutdownHook(new Thread(heuristics::stop));
    }

}
