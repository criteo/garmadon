package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.configurations.HdfsReaderConfiguration;
import com.criteo.hadoop.garmadon.hdfs.writer.FileSystemUtils;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

        final PrometheusHttpConsumerMetrics prometheusServer = new PrometheusHttpConsumerMetrics(config.getPrometheus().getPort());

        FileSystem fs = null;
        try {
            final String baseTemporaryHdfsDir = config.getHdfs().getBaseTemporaryDir();
            Path finalHdfsDir = new Path(config.getHdfs().getFinalDir());

            try {
                // Required if using ViewFs to resolve hdfs path and use DistributedFileSystem
                FileSystem fsTmp = finalHdfsDir.getFileSystem(HDFS_CONF);
                fsTmp.mkdirs(finalHdfsDir); // Create final folder if not exist before to resolve path
                finalHdfsDir = fsTmp.resolvePath(finalHdfsDir);
                fs = finalHdfsDir.getFileSystem(HDFS_CONF);
            } catch (IOException e) {
                LOGGER.error("Could not initialize HDFS", e);
                throw e;
            }

            if (!(fs instanceof DistributedFileSystem || fs instanceof LocalFileSystem)) {
                throw new UnsupportedOperationException("Filesystem of type " + fs.getScheme() + " is not supported. Only hdfs and file ones are supported");
            }

            try {
                FileSystemUtils.ensureDirectoriesExist(Collections.singletonList(finalHdfsDir), fs);
            } catch (IOException e) {
                LOGGER.error("Couldn't ensure base directories exist, exiting", e);
                throw e;
            }

            LOGGER.info("Final HDFS dir: {}", finalHdfsDir.toUri());

            ReaderFactory readerFactory = new ReaderFactory(config);

            Path finalHdfsDirCapture = finalHdfsDir;
            FileSystem fsCapture = fs;
            List<GarmadonReader> readers = IntStream.range(0, Integer.max(1, config.getParallelism())).mapToObj(i -> {
                Path temporaryHdfsDir = new Path(baseTemporaryHdfsDir, UUID.randomUUID().toString());
                try {
                    FileSystemUtils.ensureDirectoriesExist(Collections.singletonList(temporaryHdfsDir), fsCapture);
                } catch (IOException e) {
                    LOGGER.error("Couldn't ensure base directories exist, exiting", e);
                    exit(1);
                }

                try {
                    fsCapture.deleteOnExit(temporaryHdfsDir);
                } catch (IOException e) {
                    exit(1);
                }

                LOGGER.info("Temporary HDFS dir: {}", temporaryHdfsDir.toUri());

                final KafkaConsumer<String, byte[]> kafkaConsumer = kafkaConsumer(config);

                return readerFactory.create(kafkaConsumer, config.getKafka().getTopics(),
                    fsCapture, finalHdfsDirCapture, temporaryHdfsDir);
            }).collect(Collectors.toList());

            CompletableFuture<Object> oneReaderEnds = CompletableFuture.anyOf(
                readers.stream().map(GarmadonReader::startReading).toArray(CompletableFuture[]::new)
            )
                .exceptionally(t -> null);

            CompletableFuture<Void> theEnd = oneReaderEnds.thenCompose(ignored ->
                CompletableFuture.allOf(readers.stream().map(GarmadonReader::stopReading).toArray(CompletableFuture[]::new)));

            try {
                theEnd.join();
            } catch (Exception e) {
                LOGGER.error("Reader thread interrupted", e);
            }

        } finally {
            if (fs != null) fs.close();
            prometheusServer.terminate();
        }

    }

    public static KafkaConsumer<String, byte[]> kafkaConsumer(HdfsReaderConfiguration conf) {
        final Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);

        props.putAll(conf.getKafka().getSettings());

        // Auto-commit for lag monitoring, since we don't use Kafka commits
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        return new KafkaConsumer<>(props);
    }

}
