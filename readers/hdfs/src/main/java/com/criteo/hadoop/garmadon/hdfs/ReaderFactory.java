package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.event.proto.*;
import com.criteo.hadoop.garmadon.hdfs.configurations.HdfsReaderConfiguration;
import com.criteo.hadoop.garmadon.hdfs.hive.HiveClient;
import com.criteo.hadoop.garmadon.hdfs.kafka.OffsetResetter;
import com.criteo.hadoop.garmadon.hdfs.kafka.PartitionsPauseStateHandler;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.*;
import com.criteo.hadoop.garmadon.hdfs.writer.*;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.any;
import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasType;

public class ReaderFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderFactory.class);

    private static final AtomicInteger READER_IDX = new AtomicInteger(0);

    private static Map<Integer, GarmadonEventDescriptor> typeToEventDescriptor;

    {
        Map<Integer, GarmadonEventDescriptor> out = new HashMap<>();
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FS_EVENT, "fs", EventsWithHeader.FsEvent.class,
            DataAccessEventProtos.FsEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.GC_EVENT, "gc", EventsWithHeader.GCStatisticsData.class,
            JVMStatisticsEventsProtos.GCStatisticsData.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.CONTAINER_MONITORING_EVENT, "container",
            EventsWithHeader.ContainerResourceEvent.class, ContainerEventProtos.ContainerResourceEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_EVENT, "spark_stage",
            EventsWithHeader.SparkStageEvent.class, SparkEventProtos.StageEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_STAGE_STATE_EVENT, "spark_stage_state",
            EventsWithHeader.SparkStageStateEvent.class, SparkEventProtos.StageStateEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_EXECUTOR_STATE_EVENT, "spark_executor",
            EventsWithHeader.SparkExecutorStateEvent.class, SparkEventProtos.ExecutorStateEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_TASK_EVENT, "spark_task",
            EventsWithHeader.SparkTaskEvent.class, SparkEventProtos.TaskEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_RDD_STORAGE_STATUS_EVENT, "spark_rdd_storage_status",
            EventsWithHeader.SparkRddStorageStatus.class, SparkEventProtos.RDDStorageStatus.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.SPARK_EXECUTOR_STORAGE_STATUS_EVENT, "spark_executor_storage_status",
            EventsWithHeader.SparkExecutorStorageStatus.class, SparkEventProtos.ExecutorStorageStatus.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.APPLICATION_EVENT, "application_event",
            EventsWithHeader.ApplicationEvent.class, ResourceManagerEventProtos.ApplicationEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.CONTAINER_EVENT, "container_event",
            EventsWithHeader.ContainerEvent.class, ResourceManagerEventProtos.ContainerEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_JOB_MANAGER_EVENT, "flink_job_manager",
            EventsWithHeader.FlinkJobManagerEvent.class, FlinkEventProtos.JobManagerEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_JOB_EVENT, "flink_job",
            EventsWithHeader.FlinkJobEvent.class, FlinkEventProtos.JobEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_TASK_MANAGER_EVENT, "flink_task_manager",
            EventsWithHeader.FlinkTaskManagerEvent.class, FlinkEventProtos.TaskManagerEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_TASK_EVENT, "flink_task",
            EventsWithHeader.FlinkTaskEvent.class, FlinkEventProtos.TaskEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_OPERATOR_EVENT, "flink_operator",
            EventsWithHeader.FlinkOperatorEvent.class, FlinkEventProtos.OperatorEvent.newBuilder());
        addTypeMapping(out, GarmadonSerialization.TypeMarker.FLINK_KAFKA_CONSUMER_EVENT, "flink_kafka_consumer",
            EventsWithHeader.FlinkKafkaConsumerEvent.class, FlinkEventProtos.KafkaConsumerEvent.newBuilder());
        typeToEventDescriptor = Collections.unmodifiableMap(out);
    }

    private final int maxTmpFileOpenRetries;
    private final int messagesBeforeExpiringWriters;
    private final Duration writersExpirationDelay;
    private final Duration expirerPeriod;
    private final Duration heartbeatPeriod;
    private final Duration tmpFileOpenRetryPeriod;
    private final int sizeBeforeFlushingTmp;
    private final int backlogDays;
    private final String kafkaCluster;
    private final Boolean createHiveTable;
    private final String driverName;
    private final String hiveJdbcUrl;
    private final String hiveDatabase;

    public ReaderFactory(HdfsReaderConfiguration conf) {
        maxTmpFileOpenRetries = conf.getHdfs().getMaxTmpFileOpenRetries();
        messagesBeforeExpiringWriters = conf.getHdfs().getMessagesBeforeExpiringWriters();
        writersExpirationDelay = Duration.ofMinutes(conf.getHdfs().getWritersExpirationDelay());
        expirerPeriod = Duration.ofSeconds(conf.getHdfs().getExpirerPeriod());
        heartbeatPeriod = Duration.ofSeconds(conf.getHdfs().getHeartbeatPeriod());
        tmpFileOpenRetryPeriod = Duration.ofSeconds(conf.getHdfs().getTmpFileOpenRetryPeriod());
        sizeBeforeFlushingTmp = conf.getHdfs().getSizeBeforeFlushingTmp();
        backlogDays = conf.getHdfs().getBacklogDays();
        kafkaCluster = conf.getKafka().getCluster();
        createHiveTable = conf.getHive().isCreateHiveTable();
        driverName = conf.getHive().getDriverName();
        hiveJdbcUrl = conf.getHive().getHiveJdbcUrl();
        hiveDatabase = conf.getHive().getHiveDatabase();
    }

    private static void addTypeMapping(Map<Integer, GarmadonEventDescriptor> out,
                                       Integer type, String path, Class<? extends Message> clazz, Message.Builder emptyMessageBuilder) {
        out.put(type, new GarmadonEventDescriptor(path, clazz, emptyMessageBuilder));
    }

    public GarmadonReader create(KafkaConsumer<String, byte[]> kafkaConsumer, FileSystem fs, Path finalHdfsDir, Path temporaryHdfsDir) {

        int idx = READER_IDX.getAndIncrement();

        final GarmadonReader.Builder readerBuilder = GarmadonReader.Builder.stream(kafkaConsumer);
        final Collection<PartitionedWriter<Message>> writers = new ArrayList<>();
        final PartitionedWriter.Expirer expirer = new PartitionedWriter.Expirer<>(writers, expirerPeriod);
        final HeartbeatConsumer heartbeat = new HeartbeatConsumer<>(writers, heartbeatPeriod);
        final PartitionsPauseStateHandler pauser = new PartitionsPauseStateHandler(kafkaConsumer);

        HiveClient hiveClient = null;
        if (createHiveTable) {
            try {
                hiveClient = new HiveClient(driverName, hiveJdbcUrl, hiveDatabase, new Path(finalHdfsDir, "hive").toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        for (Map.Entry<Integer, GarmadonEventDescriptor> out : typeToEventDescriptor.entrySet()) {
            final Integer eventType = out.getKey();
            final String eventName = out.getValue().getPath();
            final Class<? extends Message> clazz = out.getValue().getClazz();
            final Message.Builder emptyMessageBuilder = out.getValue().getEmptyMessageBuilder();
            final BiFunction<Integer, LocalDateTime, ExpiringConsumer<Message>> consumerBuilder;
            final Path finalEventDir = new Path(finalHdfsDir, eventName);
            final OffsetComputer offsetComputer = new HdfsOffsetComputer(fs, finalEventDir, kafkaCluster, backlogDays);

            // When it's day D + 2h, checkpoint for day D - 1m
            final DelayedDailyPathComputer delayedPathComputer = new DelayedDailyPathComputer(Duration.ofHours(24 + 2));
            final Checkpointer checkpointer = new FsBasedCheckpointer(fs,
                (partition, instant) -> {
                    Path dayDir = new Path(finalEventDir,
                        delayedPathComputer.apply(instant.atZone(ZoneId.of("UTC"))));

                    return new Path(dayDir, "." + partition.toString() + ".done");
                });

            consumerBuilder = buildMessageConsumerBuilder(fs, new Path(temporaryHdfsDir, eventName),
                finalEventDir, clazz, offsetComputer, pauser, eventName, hiveClient);

            final PartitionedWriter<Message> writer = new PartitionedWriter<>(
                consumerBuilder, offsetComputer, eventName, emptyMessageBuilder, checkpointer);

            readerBuilder.intercept(hasType(eventType), buildGarmadonMessageHandler(writer, eventName));

            writers.add(writer);
        }

        final List<ConsumerRebalanceListener> kafkaConsumerRebalanceListeners = new ArrayList<>();
        kafkaConsumerRebalanceListeners.add(new OffsetResetter<>(kafkaConsumer, heartbeat::dropPartition, writers));
        kafkaConsumerRebalanceListeners.add(pauser);
        kafkaConsumerRebalanceListeners.add(new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(tp -> PrometheusMetrics.clearPartitionCollectors(tp.partition()));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });

        // We need to build a meta listener as only the last call to #subscribe wins
        kafkaConsumer.subscribe(Collections.singleton(GarmadonReader.GARMADON_TOPIC),
            new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    kafkaConsumerRebalanceListeners.forEach(listener -> listener.onPartitionsRevoked(partitions));
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    kafkaConsumerRebalanceListeners.forEach(listener -> listener.onPartitionsAssigned(partitions));
                }
            });

        readerBuilder.intercept(any(), msg -> {
            heartbeat.handle(msg);

            CommittableOffset offset = msg.getCommittableOffset();

            Gauge.Child gauge = PrometheusMetrics.currentRunningOffsetsGauge("global", offset.getPartition());
            gauge.set(offset.getOffset());
        });

        GarmadonReader reader = readerBuilder.build(false);

        Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (thread, e) -> {
            LOGGER.error("Interrupting reader " + idx, e);
            reader.stopReading().whenComplete((t, ex) -> {
                expirer.stop().join();
                heartbeat.stop().join();
            });
        };

        expirer.start(uncaughtExceptionHandler, "expirer-" + idx);
        heartbeat.start(uncaughtExceptionHandler, "heartbeat-" + idx);

        Runtime.getRuntime().addShutdownHook(new Thread(reader::stopReading));

        return reader;
    }

    private BiFunction<Integer, LocalDateTime, ExpiringConsumer<Message>> buildMessageConsumerBuilder(
        FileSystem fs, Path temporaryHdfsDir, Path finalHdfsDir, Class<? extends Message> clazz,
        OffsetComputer offsetComputer, PartitionsPauseStateHandler partitionsPauser, String eventName,
        HiveClient hiveClient) {
        Counter.Child tmpFileOpenFailures = PrometheusMetrics.tmpFileOpenFailuresCounter(eventName);
        Counter.Child tmpFilesOpened = PrometheusMetrics.tmpFilesOpened(eventName);

        return (partition, dayStartTime) -> {
            final String uniqueFileName = UUID.randomUUID().toString();
            final String additionalInfo = String.format("Date = %s, Event type = %s", dayStartTime,
                clazz.getSimpleName());

            for (int i = 0; i < maxTmpFileOpenRetries; ++i) {
                final Path tmpFilePath = new Path(temporaryHdfsDir, uniqueFileName);
                final ParquetWriter<Message> protoWriter;
                final ExtraMetadataWriteSupport<Message> extraMetadataWriteSupport;

                try {
                    extraMetadataWriteSupport = new ExtraMetadataWriteSupport<>(new ProtoWriteSupport<>(clazz));
                    protoWriter = new ParquetWriter<>(tmpFilePath, extraMetadataWriteSupport, CompressionCodecName.GZIP,
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
                    protoWriter, tmpFilePath, finalHdfsDir, fs, offsetComputer, dayStartTime, eventName, extraMetadataWriteSupport, partition, hiveClient),
                    writersExpirationDelay, messagesBeforeExpiringWriters);
            }

            // There's definitely something wrong, potentially the whole instance, so stop trying
            throw new FileSystemNotFoundException(String.format(
                "Failed opening a temporary file after %d retries: %s",
                maxTmpFileOpenRetries, additionalInfo));
        };
    }

    private GarmadonReader.GarmadonMessageHandler buildGarmadonMessageHandler(PartitionedWriter<Message> writer,
                                                                              String eventName) {
        return msg -> {
            final CommittableOffset offset = msg.getCommittableOffset();
            final Counter.Child messagesWritingFailures = PrometheusMetrics.messageWritingFailuresCounter(eventName, offset.getPartition());
            final Counter.Child messagesWritten = PrometheusMetrics.messageWrittenCounter(eventName, offset.getPartition());

            Gauge.Child gauge = PrometheusMetrics.currentRunningOffsetsGauge(eventName, offset.getPartition());
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

}
