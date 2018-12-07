package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.kafka.OffsetResetter;
import com.criteo.hadoop.garmadon.hdfs.kafka.PartitionsPauseStateHandler;
import com.criteo.hadoop.garmadon.hdfs.offset.*;
import com.criteo.hadoop.garmadon.hdfs.writer.ExpiringConsumer;
import com.criteo.hadoop.garmadon.hdfs.writer.ProtoParquetWriterWithOffset;
import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private static final Map<String, String> defaultPropertiesValue = new HashMap<>();
    private static final Map<String, String> defaultPropertiesDescription = new HashMap<>();

    static {
        defaultPropertiesValue.put(MESSAGES_BEFORE_EXPIRING_WRITERS, "500000");
        defaultPropertiesValue.put(WRITERS_EXPIRATION_DELAY, "30");
        defaultPropertiesValue.put(EXPIRER_PERIOD, "30");
        defaultPropertiesValue.put(HEARTBEAT_PERIOD, "30");
        defaultPropertiesValue.put(MAX_TMP_FILE_OPEN_RETRIES, "10");
        defaultPropertiesValue.put(TMP_FILE_OPEN_RETRY_PERIOD, "30");
    }

    static {
        defaultPropertiesDescription.put(MESSAGES_BEFORE_EXPIRING_WRITERS,
                String.format("Soft limit (see '%s') for number of messages before writing final files", EXPIRER_PERIOD));
        defaultPropertiesDescription.put(WRITERS_EXPIRATION_DELAY,
                String.format("Soft limit (see '%s') for time since opening before writing final files (in minutes)",
                        EXPIRER_PERIOD));
        defaultPropertiesDescription.put(EXPIRER_PERIOD,
                String.format("How often the exporter should try to commit files to their final destination, based " +
                "on '%s' and '%s' (in seconds)", MESSAGES_BEFORE_EXPIRING_WRITERS, WRITERS_EXPIRATION_DELAY));
        defaultPropertiesDescription.put(HEARTBEAT_PERIOD,
                "How often a placeholder file should be committed to keep track of maximum offset with no message for" +
                        " a given event type (in seconds)");
        defaultPropertiesDescription.put(MAX_TMP_FILE_OPEN_RETRIES,
                "The maximum number of times failing to open a temporary file (in a row) before aborting the program");
        defaultPropertiesDescription.put(TMP_FILE_OPEN_RETRY_PERIOD,
                "How long to wait between failures to open a temporary file for writing (in seconds)");
    }

    private static int maxTmpFileOpenRetries;
    private static int messagesBeforeExpiringWriters;
    private static Duration writersExpirationDelay;
    private static Duration expirerPeriod;
    private static Duration heartbeatPeriod;
    private static Duration tmpFileOpenRetryPeriod;

    protected HdfsExporter() {
        throw new UnsupportedOperationException();
    }

    /**
     * args[0]: Kafka connection string
     * args[1]: Kafka group
     * args[2]: Temporary HDFS directory
     * args[3]: Final HDFS directory
     */
    public static void main(String[] args) {
        setupProperties();

        if (args.length < 4) {
            printHelp();
            return;
        }

        final String kafkaConnectionString = args[0];
        final String kafkaGroupId = args[1];
        final String baseTemporaryHdfsDir = args[2];
        final Path finalHdfsDir = new Path(args[3]);

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

        final KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        final GarmadonReader.Builder readerBuilder = GarmadonReader.Builder.stream(kafkaConsumer);
        final Collection<PartitionedWriter<Message>> writers = new ArrayList<>();
        final PartitionedWriter.Expirer expirer = new PartitionedWriter.Expirer<>(writers, expirerPeriod);
        final HeartbeatConsumer heartbeat = new HeartbeatConsumer<>(writers, heartbeatPeriod);
        final Map<Integer, Map.Entry<String, Class<? extends Message>>> typeToDirAndClass = getTypeToDirAndClass();
        final Path temporaryHdfsDir = new Path(baseTemporaryHdfsDir, UUID.randomUUID().toString());

        ensureDirectoriesExist(Arrays.asList(temporaryHdfsDir, finalHdfsDir), fs);

        LOGGER.info("Temporary HDFS dir: {}", temporaryHdfsDir.toUri());
        LOGGER.info("Final HDFS dir: {}", finalHdfsDir.toUri());

        final PartitionsPauseStateHandler pauser = new PartitionsPauseStateHandler(kafkaConsumer);

        for (Map.Entry<Integer, Map.Entry<String, Class<? extends Message>>> out: typeToDirAndClass.entrySet()) {
            final Integer eventType = out.getKey();
            final String path = out.getValue().getKey();
            final Class<? extends Message> clazz = out.getValue().getValue();
            final Function<LocalDateTime, ExpiringConsumer<Message>> consumerBuilder;
            final Path finalEventDir = new Path(finalHdfsDir, path);
            final OffsetComputer offsetComputer = new HdfsOffsetComputer(fs, finalEventDir);

            consumerBuilder = buildMessageConsumerBuilder(fs, new Path(temporaryHdfsDir, path),
                    finalEventDir, clazz, offsetComputer, pauser);

            final PartitionedWriter<Message> writer = new PartitionedWriter<>(
                    consumerBuilder, offsetComputer);

            readerBuilder.intercept(hasType(eventType), buildGarmadonMessageHandler(writer));

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

        readerBuilder.intercept(any(), heartbeat);

        final GarmadonReader garmadonReader = readerBuilder.build(false);

        expirer.start();
        heartbeat.start();

        try {
            garmadonReader.startReading().join();
        } catch (Exception e) {
            LOGGER.error("Reader thread interrupted", e);
        }

        expirer.stop().join();
        heartbeat.stop().join();
    }

    private static void setupProperties() {
        maxTmpFileOpenRetries = Integer.valueOf(System.getProperty("maxTmpFileOpenRetries", "20"));
        messagesBeforeExpiringWriters = Integer.valueOf(
                System.getProperty("messagesBeforeExpiringWriters", "50000"));
        writersExpirationDelay = Duration.ofMinutes(Integer.valueOf(
                System.getProperty("writersExpirationDelay", "20")));
        expirerPeriod = Duration.ofSeconds(Integer.valueOf(
                System.getProperty("expirerPeriod", "30")));
        heartbeatPeriod = Duration.ofSeconds(Integer.valueOf(
                System.getProperty("heartbeatPeriod", "30")));
        tmpFileOpenRetryPeriod = Duration.ofSeconds(Integer.valueOf(
                System.getProperty("tmpFileOpenRetryPeriod", "30")));
    }

    /**
     * Ensure paths exist and are directories. Otherwise create them.
     *
     * @param dirs                      Directories that need to exist
     * @param fs                        Filesystem to which these directories should belong
     * @throws IllegalStateException    When failing to ensure directories existence
     */
    private static void ensureDirectoriesExist(List<Path> dirs, FileSystem fs) {
        try {
            for (Path dir : dirs) {
                if (!fs.exists(dir)) fs.mkdirs(dir);

                if (!fs.isDirectory(dir)) {
                    throw new IllegalStateException(
                            String.format("Couldn't ensure directory %s exists: not a directory", dir.getName()));
                }
            }
        } catch (IOException e) {
            final String dirsString = dirs.stream().map(Path::getName).collect(Collectors.joining());
            throw new IllegalStateException(String.format("Couldn't ensure directories %s exist", dirsString), e);
        }
    }

    private static Function<LocalDateTime, ExpiringConsumer<Message>> buildMessageConsumerBuilder(
            FileSystem fs, Path temporaryHdfsDir, Path finalHdfsDir, Class<? extends Message> clazz,
            OffsetComputer offsetComputer, PartitionsPauseStateHandler partitionsPauser) {
        return (dayStartTime) -> {
            final String uniqueFileName = UUID.randomUUID().toString();
            final String additionalInfo = String.format("Date = %s, Event type = %s", dayStartTime,
                    clazz.getSimpleName());

            for (int i = 0; i < maxTmpFileOpenRetries; ++i) {
                final Path tmpFilePath = new Path(temporaryHdfsDir, uniqueFileName);
                final ProtoParquetWriter<Message> protoWriter;

                try {
                    protoWriter = new ProtoParquetWriter<>(tmpFilePath, clazz);
                } catch (IOException e) {
                    LOGGER.warn("Could not initialize writer ({})", additionalInfo, e);

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
                        protoWriter, tmpFilePath, finalHdfsDir, fs, offsetComputer, dayStartTime),
                        writersExpirationDelay, messagesBeforeExpiringWriters);
            }

            // There's definitely something wrong, potentially the whole instance, so stop trying
            throw new FileSystemNotFoundException(String.format(
                    "Failed opening a temporary file after %d retries: %s",
                    maxTmpFileOpenRetries, additionalInfo));
        };
    }

    private static GarmadonReader.GarmadonMessageHandler buildGarmadonMessageHandler(PartitionedWriter<Message> writer) {
        return (msg) -> {
            final CommittableOffset offset = msg.getCommittableOffset();

            try {
                writer.write(Instant.now(), offset,
                        ProtoConcatenator.concatToProtobuf(Arrays.asList(msg.getHeader(), (Message) msg.getBody())));
            } catch (IOException e) {
                // We accept losing messages every now and then, but still log failures
                LOGGER.warn("Couldn't write a message", e);
            }
        };
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.parquet.HdfsExporter " +
                "kafka_connection_string kafka_group temp_dir final_dir");

        System.out.println();
        System.out.println("Properties settable via -D:");

        for (String parameter: Arrays.asList(MESSAGES_BEFORE_EXPIRING_WRITERS, WRITERS_EXPIRATION_DELAY, EXPIRER_PERIOD,
                HEARTBEAT_PERIOD, MAX_TMP_FILE_OPEN_RETRIES, TMP_FILE_OPEN_RETRY_PERIOD)) {
            System.out.println(String.format(
                    " * %s: %s (default value = %s)", parameter, defaultPropertiesDescription.get(parameter),
                    defaultPropertiesValue.get(parameter)));
        }

        for (Object parameter: Arrays.asList(messagesBeforeExpiringWriters, writersExpirationDelay, expirerPeriod,
                heartbeatPeriod, maxTmpFileOpenRetries, tmpFileOpenRetryPeriod)) {
            System.out.println(parameter.toString());
        }
    }

    private static Map<Integer, Map.Entry<String, Class<? extends Message>>> getTypeToDirAndClass() {
        final Map<Integer, Map.Entry<String, Class<? extends Message>>> out = new HashMap<>();

        addTypeMapping(out, GarmadonSerialization.TypeMarker.PATH_EVENT, "path", EventsWithHeader.PathEvent.class);
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
