package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.event.proto.*;
import com.criteo.hadoop.garmadon.hdfs.configurations.HdfsConfiguration;
import com.criteo.hadoop.garmadon.hdfs.configurations.HdfsReaderConfiguration;
import com.criteo.hadoop.garmadon.hdfs.kafka.OffsetResetter;
import com.criteo.hadoop.garmadon.hdfs.kafka.PartitionsPauseStateHandler;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.HdfsOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.offset.HeartbeatConsumer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.writer.ExpiringConsumer;
import com.criteo.hadoop.garmadon.hdfs.writer.FileSystemUtils;
import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.hdfs.writer.ProtoParquetWriterWithOffset;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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

    static {
        // Configuration for underlying packages using JUL
        String path = HdfsExporter.class.getClassLoader()
            .getResource("logging.properties")
            .getFile();
        System.setProperty("java.util.logging.config.file", path);
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
     * @param args: args[0]: Kafka connection string
     *              args[1]: Kafka group
     *              args[2]: Temporary HDFS directory
     *              args[3]: Final HDFS directory
     *              args[4]: Prometheus port
     * @throws IOException in case of error during config loading
     */
    public static void main(String[] args) throws IOException {
        HdfsReaderConfiguration config = ReaderConfiguration.loadConfig(HdfsReaderConfiguration.class);

        setupProperties(config.getHdfs());

        final String baseTemporaryHdfsDir = config.getHdfs().getBaseTemporaryDir();
        Path finalHdfsDir = new Path(config.getHdfs().getFinalDir());

        FileSystem fs = null;
        try {
            // Required if using ViewFs to resolve hdfs path and use DistributedFileSystem
            FileSystem fsTmp = finalHdfsDir.getFileSystem(HDFS_CONF);
            fsTmp.mkdirs(finalHdfsDir); // Create final folder if not exist before to resolve path
            finalHdfsDir = fsTmp.resolvePath(finalHdfsDir);
            fs = finalHdfsDir.getFileSystem(HDFS_CONF);
        } catch (IOException e) {
            LOGGER.error("Could not initialize HDFS", e);
            exit(1);
        }

        if (!(fs instanceof DistributedFileSystem || fs instanceof LocalFileSystem)) {
            throw new UnsupportedOperationException("Filesystem of type " + fs.getScheme() + " is not supported. Only hdfs and file ones are supported");
        }

        final Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);

        props.putAll(config.getKafka().getSettings());

        // Auto-commit for lag monitoring, since we don't use Kafka commits
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        final KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        final GarmadonReader.Builder readerBuilder = GarmadonReader.Builder.stream(kafkaConsumer);
        final Collection<PartitionedWriter<Message>> writers = new ArrayList<>();
        final PartitionedWriter.Expirer expirer = new PartitionedWriter.Expirer<>(writers, expirerPeriod);
        final HeartbeatConsumer heartbeat = new HeartbeatConsumer<>(writers, heartbeatPeriod);
        final Map<Integer, GarmadonEventDescriptor> typeToDirAndClass = getTypeToEventDescriptor();
        final Path temporaryHdfsDir = new Path(baseTemporaryHdfsDir, UUID.randomUUID().toString());
        final PrometheusHttpConsumerMetrics prometheusServer = new PrometheusHttpConsumerMetrics(config.getPrometheus().getPort());

        try {
            FileSystemUtils.ensureDirectoriesExist(Arrays.asList(temporaryHdfsDir, finalHdfsDir), fs);
        } catch (IOException e) {
            LOGGER.error("Couldn't ensure base directories exist, exiting", e);
            return;
        }
        fs.deleteOnExit(temporaryHdfsDir);

        LOGGER.info("Temporary HDFS dir: {}", temporaryHdfsDir.toUri());
        LOGGER.info("Final HDFS dir: {}", finalHdfsDir.toUri());

        final PartitionsPauseStateHandler pauser = new PartitionsPauseStateHandler(kafkaConsumer);

        for (Map.Entry<Integer, GarmadonEventDescriptor> out : typeToDirAndClass.entrySet()) {
            final Integer eventType = out.getKey();
            final String eventName = out.getValue().getPath();
            final Class<? extends Message> clazz = out.getValue().getClazz();
            final Message.Builder emptyMessageBuilder = out.getValue().getEmptyMessageBuilder();
            final Function<LocalDateTime, ExpiringConsumer<Message>> consumerBuilder;
            final Path finalEventDir = new Path(finalHdfsDir, eventName);
            final OffsetComputer offsetComputer = new HdfsOffsetComputer(fs, finalEventDir,
                config.getKafka().getCluster(), config.getHdfs().getBacklogDays());

            consumerBuilder = buildMessageConsumerBuilder(fs, new Path(temporaryHdfsDir, eventName),
                finalEventDir, clazz, offsetComputer, pauser, eventName);

            final PartitionedWriter<Message> writer = new PartitionedWriter<>(
                consumerBuilder, offsetComputer, eventName, emptyMessageBuilder);

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

    private static void setupProperties(HdfsConfiguration hdfsConfig) {
        maxTmpFileOpenRetries = hdfsConfig.getMaxTmpFileOpenRetries();
        messagesBeforeExpiringWriters = hdfsConfig.getMessagesBeforeExpiringWriters();
        writersExpirationDelay = Duration.ofMinutes(hdfsConfig.getWritersExpirationDelay());
        expirerPeriod = Duration.ofSeconds(hdfsConfig.getExpirerPeriod());
        heartbeatPeriod = Duration.ofSeconds(hdfsConfig.getHeartbeatPeriod());
        tmpFileOpenRetryPeriod = Duration.ofSeconds(hdfsConfig.getTmpFileOpenRetryPeriod());
        sizeBeforeFlushingTmp = hdfsConfig.getSizeBeforeFlushingTmp();
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
                writer.write(Instant.ofEpochMilli(msg.getTimestamp()), offset, msg.toProto());

                messagesWritten.inc();
            } catch (IOException e) {
                // We accept losing messages every now and then, but still log failures
                messagesWritingFailures.inc();
                LOGGER.warn("Couldn't write a message", e);
            }
        };
    }

    private static Map<Integer, GarmadonEventDescriptor> getTypeToEventDescriptor() {
        final Map<Integer, GarmadonEventDescriptor> out = new HashMap<>();

        addTypeMapping(out, GarmadonSerialization.TypeMarker.FS_EVENT, "fs", EventsWithHeader.FsEvent.class,
            DataAccessEventProtos.FsEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.GC_EVENT, "gc", EventsWithHeader.GCStatisticsData.class,
            JVMStatisticsEventsProtos.GCStatisticsData.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.CONTAINER_MONITORING_EVENT, "container",
            EventsWithHeader.ContainerEvent.class, ContainerEventProtos.ContainerResourceEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_EVENT, "spark_stage",
            EventsWithHeader.SparkStageEvent.class, SparkEventProtos.StageEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_STATE_EVENT, "spark_stage_state",
            EventsWithHeader.SparkStageStateEvent.class, SparkEventProtos.StageStateEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_EXECUTOR_STATE_EVENT, "spark_executor",
            EventsWithHeader.SparkExecutorStateEvent.class, SparkEventProtos.ExecutorStateEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_TASK_EVENT, "spark_task",
            EventsWithHeader.SparkTaskEvent.class, SparkEventProtos.TaskEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.APPLICATION_EVENT, "application_event",
            EventsWithHeader.ApplicationEvent.class, ResourceManagerEventProtos.ApplicationEvent.newBuilder());

        // TODO: handle JVM events

        return out;
    }

    private static void addTypeMapping(Map<Integer, GarmadonEventDescriptor> out,
                                       Integer type, String path, Class<? extends Message> clazz, Message.Builder emptyMessageBuilder) {
        out.put(type, new GarmadonEventDescriptor(path, clazz, emptyMessageBuilder));
    }
}
