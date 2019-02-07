package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.kafka.OffsetResetter;
import com.criteo.hadoop.garmadon.hdfs.kafka.PartitionsPauseStateHandler;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.*;
import com.criteo.hadoop.garmadon.hdfs.writer.ExpiringConsumer;
import com.criteo.hadoop.garmadon.hdfs.writer.FileSystemUtils;
import com.criteo.hadoop.garmadon.hdfs.writer.ProtoParquetWriterWithOffset;
import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.any;
import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasType;
import static java.lang.System.exit;

/**
 * Export Kafka events to HDFS
 */
public class HdfsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsExporter.class);
    private static final Configuration HDFS_CONF = new Configuration();

    private static final String MESSAGES_BEFORE_EXPIRING_WRITERS = "messagesBeforeExpiringWriters";
    private static final String WRITERS_EXPIRATION_DELAY = "writersExpirationDelay";
    private static final String EXPIRER_PERIOD = "expirerPeriod";
    private static final String HEARTBEAT_PERIOD = "heartbeatPeriod";
    private static final String MAX_TMP_FILE_OPEN_RETRIES = "maxTmpFileOpenRetries";
    private static final String TMP_FILE_OPEN_RETRY_PERIOD = "tmpFileOpenRetryPeriod";
    private static final String SIZE_BEFORE_FLUSHING_TMP = "sizeBeforeFlushingTmp";

    private static final Map<String, Integer> DEFAULT_PROPERTIES_VALUE = new HashMap<>();
    private static final Map<String, String> DEFAULT_PROPERTIES_DESCRIPTION = new HashMap<>();
    private static final List<String> PARAMETERS_NAMES = Arrays.asList(MESSAGES_BEFORE_EXPIRING_WRITERS,
            WRITERS_EXPIRATION_DELAY, EXPIRER_PERIOD, HEARTBEAT_PERIOD, MAX_TMP_FILE_OPEN_RETRIES,
            TMP_FILE_OPEN_RETRY_PERIOD, SIZE_BEFORE_FLUSHING_TMP);

    static {
        // Configuration for underlying packages using JUL
        String path = HdfsExporter.class.getClassLoader()
                .getResource("logging.properties")
                .getFile();
        System.setProperty("java.util.logging.config.file", path);

        DEFAULT_PROPERTIES_VALUE.put(MESSAGES_BEFORE_EXPIRING_WRITERS, 3_000_000);
        DEFAULT_PROPERTIES_VALUE.put(WRITERS_EXPIRATION_DELAY, 30);
        DEFAULT_PROPERTIES_VALUE.put(EXPIRER_PERIOD, 30);
        // This is not a multiple of 30s on purpose, to avoid closing files at the same time as running heartbeats
        DEFAULT_PROPERTIES_VALUE.put(HEARTBEAT_PERIOD, 320);
        DEFAULT_PROPERTIES_VALUE.put(MAX_TMP_FILE_OPEN_RETRIES, 10);
        DEFAULT_PROPERTIES_VALUE.put(TMP_FILE_OPEN_RETRY_PERIOD, 30);
        DEFAULT_PROPERTIES_VALUE.put(SIZE_BEFORE_FLUSHING_TMP, 16);
    }

    static {
        DEFAULT_PROPERTIES_DESCRIPTION.put(MESSAGES_BEFORE_EXPIRING_WRITERS,
                String.format("Soft limit (see '%s') for number of messages before writing final files", EXPIRER_PERIOD));
        DEFAULT_PROPERTIES_DESCRIPTION.put(WRITERS_EXPIRATION_DELAY,
                String.format("Soft limit (see '%s') for time since opening before writing final files (in minutes)",
                        EXPIRER_PERIOD));
        DEFAULT_PROPERTIES_DESCRIPTION.put(EXPIRER_PERIOD,
                String.format("How often the exporter should try to commit files to their final destination, based " +
                "on '%s' and '%s' (in seconds)", MESSAGES_BEFORE_EXPIRING_WRITERS, WRITERS_EXPIRATION_DELAY));
        DEFAULT_PROPERTIES_DESCRIPTION.put(HEARTBEAT_PERIOD,
                "How often a placeholder file should be committed to keep track of maximum offset with no message for" +
                        " a given event type (in seconds)");
        DEFAULT_PROPERTIES_DESCRIPTION.put(MAX_TMP_FILE_OPEN_RETRIES,
                "Maximum number of times failing to open a temporary file (in a row) before aborting the program");
        DEFAULT_PROPERTIES_DESCRIPTION.put(TMP_FILE_OPEN_RETRY_PERIOD,
                "How long to wait between failures to open a temporary file for writing (in seconds)");
        DEFAULT_PROPERTIES_DESCRIPTION.put(SIZE_BEFORE_FLUSHING_TMP,
                "How big the temporary files buffer should be before flushing (in MB)");
    }

    private static int maxTmpFileOpenRetries;
    private static int messagesBeforeExpiringWriters;
    private static Duration writersExpirationDelay;
    private static Duration expirerPeriod;
    private static Duration heartbeatPeriod;
    private static Duration tmpFileOpenRetryPeriod;
    private static int sizeBeforeFlushingTmp;

    protected HdfsExporter() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param args:
     * args[0]: Kafka connection string
     * args[1]: Kafka group
     * args[2]: Temporary HDFS directory
     * args[3]: Final HDFS directory
     * args[4]: Prometheus port
     */
    public static void main(String[] args) {
        setupProperties();

        if (args.length < 5) {
            printHelp();
            return;
        }

        final String kafkaConnectionString = args[0];
        final String kafkaGroupId = args[1];
        final String baseTemporaryHdfsDir = args[2];
        final Path finalHdfsDir = new Path(args[3]);
        final int prometheusPort = Integer.parseInt(args[4]);

        FileSystem fs = null;
        try {
            fs = finalHdfsDir.getFileSystem(HDFS_CONF);
        } catch (IOException e) {
            LOGGER.error("Could not initialize HDFS", e);
            exit(1);
        }

        final Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionString);

        // Auto-commit for lag monitoring, since we don't use Kafka commits
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        final KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        final GarmadonReader.Builder readerBuilder = GarmadonReader.Builder.stream(kafkaConsumer);
        final Collection<PartitionedWriter<Message>> writers = new ArrayList<>();
        final PartitionedWriter.Expirer expirer = new PartitionedWriter.Expirer<>(writers, expirerPeriod);
        final HeartbeatConsumer heartbeat = new HeartbeatConsumer<>(writers, heartbeatPeriod);
        final Map<Integer, Map.Entry<String, Class<? extends Message>>> typeToDirAndClass = getTypeToDirAndClass();
        final Path temporaryHdfsDir = new Path(baseTemporaryHdfsDir, UUID.randomUUID().toString());
        final PrometheusHttpConsumerMetrics prometheusServer = new PrometheusHttpConsumerMetrics(prometheusPort);

        try {
            FileSystemUtils.ensureDirectoriesExist(Arrays.asList(temporaryHdfsDir, finalHdfsDir), fs);
        } catch (IOException e) {
            LOGGER.error("Couldn't ensure base directories exist, exiting", e);
            return;
        }

        LOGGER.info("Temporary HDFS dir: {}", temporaryHdfsDir.toUri());
        LOGGER.info("Final HDFS dir: {}", finalHdfsDir.toUri());

        final PartitionsPauseStateHandler pauser = new PartitionsPauseStateHandler(kafkaConsumer);

        for (Map.Entry<Integer, Map.Entry<String, Class<? extends Message>>> out : typeToDirAndClass.entrySet()) {
            final Integer eventType = out.getKey();
            final String eventName = out.getValue().getKey();
            final Class<? extends Message> clazz = out.getValue().getValue();
            final Function<LocalDateTime, ExpiringConsumer<Message>> consumerBuilder;
            final Path finalEventDir = new Path(finalHdfsDir, eventName);
            final OffsetComputer offsetComputer = new HdfsOffsetComputer(fs, finalEventDir);

            consumerBuilder = buildMessageConsumerBuilder(fs, new Path(temporaryHdfsDir, eventName),
                    finalEventDir, clazz, offsetComputer, pauser, eventName);

            final PartitionedWriter<Message> writer = new PartitionedWriter<>(
                    consumerBuilder, offsetComputer, eventName);

            readerBuilder.intercept(hasType(eventType), buildGarmadonMessageHandler(writer, eventName));

            writers.add(writer);
        }

        final List<ConsumerRebalanceListener> listeners = Arrays.asList(
                new OffsetResetter<>(kafkaConsumer, heartbeat::dropPartition, writers), pauser);

        // We need to build a meta listener as only the last call to #subscribe wins
        kafkaConsumer.subscribe(Collections.singleton(GarmadonReader.GARMADON_TOPIC),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        listeners.forEach(listener -> listener.onPartitionsRevoked(partitions));
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        listeners.forEach(listener -> listener.onPartitionsAssigned(partitions));
                    }
                });

        readerBuilder.intercept(any(), msg -> {
            heartbeat.handle(msg);

            CommittableOffset offset = msg.getCommittableOffset();

            Gauge.Child gauge = PrometheusMetrics.buildGaugeChild(PrometheusMetrics.CURRENT_RUNNING_OFFSETS,
                    "global", offset.getPartition());
            gauge.set(offset.getOffset());
        });

        final GarmadonReader garmadonReader = readerBuilder.build(false);

        CompletableFuture<Void> readerFuture = garmadonReader.startReading();
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (thread, e) -> {
            LOGGER.error("Interrupting reader", e);
            garmadonReader.stopReading();
        };

        expirer.start(uncaughtExceptionHandler);
        heartbeat.start(uncaughtExceptionHandler);

        try {
            readerFuture.join();
        } catch (Exception e) {
            LOGGER.error("Reader thread interrupted", e);
        }

        prometheusServer.terminate();
        expirer.stop().join();
        heartbeat.stop().join();
    }

    private static void setupProperties() {
        maxTmpFileOpenRetries = Integer.getInteger(MAX_TMP_FILE_OPEN_RETRIES,
                DEFAULT_PROPERTIES_VALUE.get(MAX_TMP_FILE_OPEN_RETRIES));
        messagesBeforeExpiringWriters = Integer.getInteger(MESSAGES_BEFORE_EXPIRING_WRITERS,
                DEFAULT_PROPERTIES_VALUE.get(MESSAGES_BEFORE_EXPIRING_WRITERS));
        writersExpirationDelay = Duration.ofMinutes(Integer.getInteger(WRITERS_EXPIRATION_DELAY,
                DEFAULT_PROPERTIES_VALUE.get(WRITERS_EXPIRATION_DELAY)));
        expirerPeriod = Duration.ofSeconds(Integer.getInteger(EXPIRER_PERIOD,
                DEFAULT_PROPERTIES_VALUE.get(EXPIRER_PERIOD)));
        heartbeatPeriod = Duration.ofSeconds(Integer.getInteger(HEARTBEAT_PERIOD,
                DEFAULT_PROPERTIES_VALUE.get(HEARTBEAT_PERIOD)));
        tmpFileOpenRetryPeriod = Duration.ofSeconds(Integer.getInteger(TMP_FILE_OPEN_RETRY_PERIOD,
                DEFAULT_PROPERTIES_VALUE.get(TMP_FILE_OPEN_RETRY_PERIOD)));
        sizeBeforeFlushingTmp = Integer.getInteger(SIZE_BEFORE_FLUSHING_TMP,
                DEFAULT_PROPERTIES_VALUE.get(SIZE_BEFORE_FLUSHING_TMP));
    }


    private static Function<LocalDateTime, ExpiringConsumer<Message>> buildMessageConsumerBuilder(
            FileSystem fs, Path temporaryHdfsDir, Path finalHdfsDir, Class<? extends Message> clazz,
            OffsetComputer offsetComputer, PartitionsPauseStateHandler partitionsPauser, String eventName) {
        Counter.Child tmpFileOpenFailures = PrometheusMetrics.buildCounterChild(
                PrometheusMetrics.TMP_FILE_OPEN_FAILURES, eventName);
        Counter.Child tmpFilesOpened = PrometheusMetrics.buildCounterChild(
                PrometheusMetrics.TMP_FILES_OPENED, eventName);

        return dayStartTime -> {
            final String uniqueFileName = UUID.randomUUID().toString();
            final String additionalInfo = String.format("Date = %s, Event type = %s", dayStartTime,
                    clazz.getSimpleName());

            for (int i = 0; i < maxTmpFileOpenRetries; ++i) {
                final Path tmpFilePath = new Path(temporaryHdfsDir, uniqueFileName);
                final ProtoParquetWriter<Message> protoWriter;

                try {
                    protoWriter = new ProtoParquetWriter<>(tmpFilePath, clazz, CompressionCodecName.SNAPPY,
                            sizeBeforeFlushingTmp * 1_024 * 1_024, 1_024 * 1_024);
                    tmpFilesOpened.inc();
                } catch (IOException e) {
                    LOGGER.warn("Could not initialize writer ({})", additionalInfo, e);
                    tmpFileOpenFailures.inc();

                    try {
                        partitionsPauser.pause(clazz);
                        Thread.sleep(tmpFileOpenRetryPeriod.get(ChronoUnit.SECONDS));
                    } catch (InterruptedException interrupt) {
                        LOGGER.info("Interrupted between temp file opening retries", interrupt);
                    }

                    continue;
                }

                partitionsPauser.resume(clazz);

                return new ExpiringConsumer<>(new ProtoParquetWriterWithOffset<>(
                        protoWriter, tmpFilePath, finalHdfsDir, fs, offsetComputer, dayStartTime, eventName),
                        writersExpirationDelay, messagesBeforeExpiringWriters);
            }

            // There's definitely something wrong, potentially the whole instance, so stop trying
            throw new FileSystemNotFoundException(String.format(
                    "Failed opening a temporary file after %d retries: %s",
                    maxTmpFileOpenRetries, additionalInfo));
        };
    }

    private static GarmadonReader.GarmadonMessageHandler buildGarmadonMessageHandler(PartitionedWriter<Message> writer,
                                                                                     String eventName) {
        return msg -> {
            final CommittableOffset offset = msg.getCommittableOffset();
            final Counter.Child messagesWritingFailures = PrometheusMetrics.buildCounterChild(
                    PrometheusMetrics.MESSAGES_WRITING_FAILURES, eventName, offset.getPartition());
            final Counter.Child messagesWritten = PrometheusMetrics.buildCounterChild(
                    PrometheusMetrics.MESSAGES_WRITTEN, eventName, offset.getPartition());

            Gauge.Child gauge = PrometheusMetrics.buildGaugeChild(PrometheusMetrics.CURRENT_RUNNING_OFFSETS,
                    eventName, offset.getPartition());
            gauge.set(offset.getOffset());

            try {
                writer.write(Instant.ofEpochMilli(msg.getTimestamp()), offset, msg.getProto());

                messagesWritten.inc();
            } catch (IOException e) {
                // We accept losing messages every now and then, but still log failures
                messagesWritingFailures.inc();
                LOGGER.warn("Couldn't write a message", e);
            }
        };
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.parquet.HdfsExporter " +
                "kafka_connection_string kafka_group temp_dir final_dir prometheus_port");

        System.out.println();
        System.out.println("Properties settable via -D:");

        for (String parameter : PARAMETERS_NAMES) {
            System.out.println(String.format(
                    " * %s: %s (default value = %d)", parameter, DEFAULT_PROPERTIES_DESCRIPTION.get(parameter),
                    DEFAULT_PROPERTIES_VALUE.get(parameter)));
        }
    }

    private static Map<Integer, Map.Entry<String, Class<? extends Message>>> getTypeToDirAndClass() {
        final Map<Integer, Map.Entry<String, Class<? extends Message>>> out = new HashMap<>();

        addTypeMapping(out, GarmadonSerialization.TypeMarker.FS_EVENT, "fs", EventsWithHeader.FsEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.GC_EVENT, "gc", EventsWithHeader.GCStatisticsData.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.CONTAINER_MONITORING_EVENT, "container",
                EventsWithHeader.ContainerEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_EVENT, "spark_stage",
                EventsWithHeader.SparkStageEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_STATE_EVENT, "spark_stage_state",
                EventsWithHeader.SparkStageStateEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_EXECUTOR_STATE_EVENT, "spark_executor",
                EventsWithHeader.SparkExecutorStateEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_TASK_EVENT, "spark_task",
                EventsWithHeader.SparkTaskEvent.class);
        addTypeMapping(out, GarmadonSerialization.TypeMarker.APPLICATION_EVENT, "application_event",
                EventsWithHeader.ApplicationEvent.class);

        // TODO: handle JVM events

        return out;
    }

    private static void addTypeMapping(Map<Integer, Map.Entry<String, Class<? extends Message>>> out,
                                       Integer type, String path, Class<? extends Message> clazz) {
        out.put(type, new AbstractMap.SimpleEntry<>(path, clazz));
    }
}
