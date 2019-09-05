package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.hive.HiveClient;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Wrap an actual ProtoParquetWriter, renaming the output file properly when closing.
 *
 * @param <MESSAGE_KIND> The message to be written in Proto + Parquet
 */
public class ProtoParquetWriterWithOffset<MESSAGE_KIND extends MessageOrBuilder>
    implements CloseableWriter<MESSAGE_KIND> {

    public static final String LATEST_TIMESTAMP_META_KEY = "latest_timestamp";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoParquetWriterWithOffset.class);

    private final Path temporaryHdfsPath;
    private final ParquetWriter<MESSAGE_KIND> writer;
    private final Path finalHdfsDir;
    private final FileSystem fs;
    private final OffsetComputer fileNamer;
    private final LocalDateTime dayStartTime;
    private final String eventName;
    private final long fsBlockSize;
    private final BiConsumer<String, String> protoMetadataWriter;
    private final int partition;
    private final HiveClient hiveClient;

    private Offset latestOffset = null;
    private long latestTimestamp = 0;
    private boolean writerClosed = false;

    /**
     * @param writer            The actual Proto + Parquet writer
     * @param temporaryHdfsPath The path to which the writer will output events
     * @param finalHdfsDir      The directory to write the final output to (renamed from temporaryHdfsPath)
     * @param fs                The filesystem on which both the temporary and final files reside
     * @param fileNamer         File-naming logic for the final path
     * @param dayStartTime      The day partition the final file will go to
     * @param eventName         Event name used for logging &amp; monitoring
     */
    public ProtoParquetWriterWithOffset(ParquetWriter<MESSAGE_KIND> writer, Path temporaryHdfsPath,
                                        Path finalHdfsDir, FileSystem fs, OffsetComputer fileNamer,
                                        LocalDateTime dayStartTime, String eventName,
                                        BiConsumer<String, String> protoMetadataWriter, int partition, HiveClient hiveClient) {
        this.writer = writer;
        this.temporaryHdfsPath = temporaryHdfsPath;
        this.finalHdfsDir = finalHdfsDir;
        this.fs = fs;
        this.fileNamer = fileNamer;
        this.dayStartTime = dayStartTime;
        this.eventName = eventName;
        this.fsBlockSize = fs.getDefaultBlockSize(finalHdfsDir);
        this.protoMetadataWriter = protoMetadataWriter;
        this.partition = partition;
        this.hiveClient = hiveClient;

        initializeLatestCommittedTimestampGauge();
    }

    @Override
    public Path close() throws IOException, SQLException {
        if (latestOffset == null) {
            final String additionalInfo = String.format(" Date = %s, Temp file = %s",
                dayStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), temporaryHdfsPath.toUri());
            throw new IOException(String.format("Trying to write a zero-sized file, please fix (%s)", additionalInfo));
        }

        if (!writerClosed) {
            protoMetadataWriter.accept(LATEST_TIMESTAMP_META_KEY, String.valueOf(latestTimestamp));
            writer.close();
            writerClosed = true;
        }

        final Optional<Path> lastestExistingFinalPath = getLastestExistingFinalPath();

        long lastIndex = lastestExistingFinalPath.map(path -> fileNamer.getIndex(path.getName()) + 1).orElse(1L);

        final Path finalPath = new Path(finalHdfsDir, fileNamer.computePath(dayStartTime, lastIndex, latestOffset.getPartition()));

        if (FileSystemUtils.ensureDirectoriesExist(Collections.singleton(finalPath.getParent()), fs)) {
            // Create hive partition if not exist
            if (hiveClient != null) {
                hiveClient.createPartitionIfNotExist(eventName, writer.getFooter().getFileMetaData().getSchema(),
                    dayStartTime.format(DateTimeFormatter.ISO_DATE), finalHdfsDir.toString());
            }
        }

        if (lastestExistingFinalPath.isPresent()) {
            long blockSize = fs.getFileStatus(lastestExistingFinalPath.get()).getLen();
            if (blockSize > fsBlockSize) {
                moveToFinalPath(temporaryHdfsPath, finalPath);
            } else {
                mergeToFinalPath(lastestExistingFinalPath.get(), finalPath);
            }
        } else {
            moveToFinalPath(temporaryHdfsPath, finalPath);
        }

        PrometheusMetrics.latestCommittedOffsetGauge(eventName, latestOffset.getPartition()).set(latestOffset.getOffset());
        PrometheusMetrics.latestCommittedTimestampGauge(eventName, latestOffset.getPartition()).set(latestTimestamp);

        return finalPath;
    }

    protected void moveToFinalPath(Path tempPath, Path finalPath) throws IOException {
        if (fs instanceof DistributedFileSystem) {
            ((DistributedFileSystem) fs).rename(tempPath, finalPath, Options.Rename.OVERWRITE);
        } else {
            if (!fs.rename(tempPath, finalPath)) {
                throw new IOException(String.format("Failed to commit %s (from %s)",
                    finalPath.toUri(), tempPath));
            }
        }

        LOGGER.info("Committed {} to {}", tempPath, finalPath.toUri());
    }

    protected void mergeToFinalPath(Path lastAvailableFinalPath, Path finalPath) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(fs.getConf(), lastAvailableFinalPath)) {
            MessageType schema = reader.getFileMetaData().getSchema();
            if (!checkSchemaEquality(schema)) {
                LOGGER.warn("Schema between last available final file ({}) and temp file ({}) are not identical. We can't merge them",
                    lastAvailableFinalPath, temporaryHdfsPath);
                moveToFinalPath(temporaryHdfsPath, finalPath);
            } else {
                Path mergedTempFile = new Path(temporaryHdfsPath.toString() + ".merged");

                if (fs.isFile(mergedTempFile)) fs.delete(mergedTempFile, false);

                Map<String, String> existingMetadata = reader.getFileMetaData().getKeyValueMetaData();
                Map<String, String> newMetadata = new HashMap<>(existingMetadata);
                newMetadata.put(LATEST_TIMESTAMP_META_KEY, String.valueOf(latestTimestamp));

                ParquetFileWriter writerPF = new ParquetFileWriter(fs.getConf(), schema, mergedTempFile);
                writerPF.start();
                writerPF.appendFile(fs.getConf(), lastAvailableFinalPath);
                writerPF.appendFile(fs.getConf(), temporaryHdfsPath);
                writerPF.end(newMetadata);

                moveToFinalPath(mergedTempFile, lastAvailableFinalPath);
                try {
                    fs.delete(temporaryHdfsPath, false);
                    // This file is in a temp folder that should be deleted at exit so we should not throw exception here
                } catch (IOException ignored) {
                }
            }
        }
    }

    private boolean checkSchemaEquality(MessageType schema) throws IOException {
        MessageType schema2 = ParquetFileReader.open(fs.getConf(), temporaryHdfsPath).getFileMetaData().getSchema();

        return schema.equals(schema2);
    }


    @Override
    public void write(long timestamp, MESSAGE_KIND msg, Offset offset) throws IOException {
        if (latestOffset == null || offset.getOffset() > latestOffset.getOffset()) latestOffset = offset;

        latestTimestamp = timestamp;

        if (msg != null) {
            writer.write(msg);
        }
    }

    private Optional<Path> getLastestExistingFinalPath() throws IOException {
        final Path topicGlobPath = new Path(finalHdfsDir, fileNamer.computeTopicGlob(dayStartTime, partition));
        return Arrays.stream(fs.globStatus(topicGlobPath))
            .map(FileStatus::getPath)
            .max(Comparator.comparingLong(path -> fileNamer.getIndex(path.getName())));
    }


    private void initializeLatestCommittedTimestampGauge() {
        if (PrometheusMetrics.latestCommittedTimestampGauge(eventName, partition).get() == 0) {
            PrometheusMetrics.latestCommittedTimestampGauge(eventName, partition).set(getLatestCommittedTimestamp());
        }
    }

    private double getLatestCommittedTimestamp() {
        //there are cases for which we won't find a value for the latest committed timestamp
        // - the first time this code goes in, no file has the correct metadata
        // - for a new event type, we have no history too, so no value
        //By using the default value 'now' rather than 0, we prevent firing unnecessary alerts
        //However, if there is an actual problem and the reader never commits, it will eventually fire
        //an alert.
        long defaultValue = System.currentTimeMillis();
        try {
            Optional<Path> latestFileCommitted = getLastestExistingFinalPath();
            if (latestFileCommitted.isPresent()) {
                String timestamp = ParquetFileReader
                    .open(fs.getConf(), latestFileCommitted.get())
                    .getFooter()
                    .getFileMetaData()
                    .getKeyValueMetaData()
                    .getOrDefault(LATEST_TIMESTAMP_META_KEY, String.valueOf(defaultValue));
                return Double.valueOf(timestamp);
            } else {
                return defaultValue;
            }
        } catch (IOException e) {
            LOGGER.warn("could not get last existing final path. Defaulting latest committed timestamp to 0");
            return defaultValue;
        }
    }

}
