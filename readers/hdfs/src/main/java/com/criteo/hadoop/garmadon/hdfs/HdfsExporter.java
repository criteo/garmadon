package com.criteo.hadoop.garmadon.hdfs;

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
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
    private static final int MAX_TMP_FILE_OPEN_RETRIES = 5;

    /**
     * args[0]: Kafka connection string
     * args[1]: Kafka group
     * args[2]: Temporary HDFS directory
     * args[3]: Final HDFS directory
     */
    public static void main(String[] args) {
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
        final PartitionedWriter.Expirer expirer = new PartitionedWriter.Expirer<>(writers, Duration.ofSeconds(10));
        final Thread expirerThread = new Thread(expirer);
        final HeartbeatConsumer heartbeat = new HeartbeatConsumer<>(writers, Duration.ofSeconds(5));
        final Thread heartbeatThread = new Thread(heartbeat);
        final Map<Integer, Map.Entry<String, Class<? extends Message>>> typeToDirAndClass = getTypeToDirAndClass();
        final Random random = buildMachineUniqueRandom();
        final Path temporaryHdfsDir = new Path(baseTemporaryHdfsDir, String.valueOf(random.nextInt(Integer.MAX_VALUE)));

        ensureDirectoriesExist(Arrays.asList(temporaryHdfsDir, finalHdfsDir), fs);

        LOGGER.info("Temporary HDFS dir: {}", temporaryHdfsDir.toUri());
        LOGGER.info("Final HDFS dir: {}", finalHdfsDir.toUri());

        readerBuilder.intercept(any(), heartbeat);

        for (Map.Entry<Integer, Map.Entry<String, Class<? extends Message>>> out: typeToDirAndClass.entrySet()) {
            final Integer eventType = out.getKey();
            final String path = out.getValue().getKey();
            final Class<? extends Message> clazz = out.getValue().getValue();
            final Function<LocalDateTime, ExpiringConsumer<Message>> consumerBuilder;
            final Path finalEventDir = new Path(finalHdfsDir, path);

            consumerBuilder = buildMessageConsumerBuilder(fs, new Path(temporaryHdfsDir, path),
                    finalEventDir, clazz, random);

            final PartitionedWriter<Message> writer = new PartitionedWriter<>(
                    consumerBuilder, new HdfsOffsetComputer(fs, finalEventDir, new TrailingPartitionOffsetPattern()));

            readerBuilder.intercept(hasType(eventType), buildGarmadonMessageHandler(writer));

            writers.add(writer);
        }

        kafkaConsumer.subscribe(Collections.singleton(GarmadonReader.GARMADON_TOPIC),
                new OffsetResetter<>(kafkaConsumer, heartbeat::dropPartition, writers));

        final GarmadonReader garmadonReader = readerBuilder.build(false);

        expirerThread.start();
        heartbeatThread.start();

        try {
            garmadonReader.startReading().join();
        }
        catch (Exception e) {
            LOGGER.error("Reader thread interrupted", e);
        }

        expirer.stop();
        try {
            expirerThread.join();
        } catch (InterruptedException e) {
            LOGGER.error("Expirer thread interrupted", e);
        }

        heartbeat.stop();
        try {
            heartbeatThread.join();
        } catch (InterruptedException e) {
            LOGGER.error("Expirer thread interrupted", e);
        }
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
                if (!fs.exists(dir))
                    fs.mkdirs(dir);

                if (!fs.isDirectory(dir)) {
                    throw new IllegalStateException(
                            String.format("Couldn't ensure directory %s exists: not a directory", dir.getName()));
                }
            }
        }
        catch (IOException e) {
            final String dirsString = dirs.stream().map(Path::getName).collect(Collectors.joining());
            throw new IllegalStateException(String.format("Couldn't ensure directories %s exist", dirsString), e);
        }
    }

    /**
     * Create a unique name based on the current machine and time
     *
     * @return                          An unique name
     * @throws IllegalStateException    When the method cannot fetch required info from the machine
     */
    private static Random buildMachineUniqueRandom() {
        final String errorMsg = "Could not get network interface to compute random seed from";

        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            final long currentTime = Instant.now().getEpochSecond();
            final String seed;
            String address = null;

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();

                if (networkInterface.getHardwareAddress() != null) {
                    address = Arrays.toString(networkInterface.getHardwareAddress());
                    break;
                }
            }

            if (address == null)
                throw new IllegalStateException(errorMsg);

            seed = String.format("%s-%d", address, currentTime);

            return new Random(seed.hashCode());
        } catch (SocketException e) {
            throw new IllegalStateException(errorMsg, e);
        }
    }

    private static Function<LocalDateTime, ExpiringConsumer<Message>> buildMessageConsumerBuilder(
            FileSystem fs, Path temporaryHdfsDir, Path finalHdfsDir, Class<? extends Message> clazz, Random random) {
        return (dayStartTime) -> {
            final long uniqueFileName = random.nextLong();

            for (int i = 0; i < MAX_TMP_FILE_OPEN_RETRIES; ++i) {
                final Path tmpFilePath = new Path(temporaryHdfsDir, String.valueOf(uniqueFileName));
                final ProtoParquetWriter<Message> protoWriter;

                try {
                    protoWriter = new ProtoParquetWriter<>(tmpFilePath, clazz);
                } catch (IOException e) {
                    final String additionalInfo = String.format("Date = %s, Event type = %s", dayStartTime,
                            clazz.getSimpleName());
                    LOGGER.warn("Could not initialize writer ({})", additionalInfo, e);

                    continue;
                }

                return new ExpiringConsumer<>(new ProtoParquetWriterWithOffset<>(protoWriter, tmpFilePath, finalHdfsDir, fs,
                        new TrailingPartitionOffsetFileNamer(), dayStartTime),
                        Duration.ofSeconds(30), 10);
            }

            // FIXME: What to do?
            return null;
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

        // TODO: handle JVM events

        return out;
    }

    private static void addTypeMapping(Map<Integer, Map.Entry<String, Class<? extends Message>>> out,
                                       Integer type, String path, Class<? extends Message> clazz) {
        out.put(type, new AbstractMap.SimpleEntry<>(path, clazz));
    }
}
