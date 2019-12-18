package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private final ParquetWriter<MESSAGE_KIND> writer;
    private final Path finalHdfsDir;
    private final FileSystem fs;
    private final OffsetComputer fileNamer;
    private final LocalDateTime dayStartTime;
    private final String eventName;

    private final Path temporaryHdfsPath;
    private final long fsBlockSize;
    private final BiConsumer<String, String> protoMetadataWriter;
    private final int partition;

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
                                        BiConsumer<String, String> protoMetadataWriter, int partition) {
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

        initializeLatestCommittedTimestampGauge();
    }

    @Override
    public Path close() throws IOException {
        if (latestOffset == null) {
            final String additionalInfo = String.format(" Date = %s, Temp file = %s",
                dayStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), temporaryHdfsPath.toUri());
            throw new IOException(String.format("Trying to write a zero-sized file, please fix (%s)", additionalInfo));
        }

        closeWriter();

        final Optional<Path> lastestExistingFinalPath = getLastestExistingFinalPath();

        long lastIndex = lastestExistingFinalPath.map(path -> fileNamer.getIndex(path.getName()) + 1).orElse(1L);

        final Path finalPath = new Path(finalHdfsDir, fileNamer.computePath(dayStartTime, lastIndex, latestOffset.getPartition()));

        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(finalPath.getParent()), fs);

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

    protected void closeWriter() throws IOException {
        if (!writerClosed) {
            protoMetadataWriter.accept(LATEST_TIMESTAMP_META_KEY, String.valueOf(latestTimestamp));
            writer.close();
            writerClosed = true;
        }
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

                try (
                    ParquetWriter<Object> writerPF = new ParquetWriter(mergedTempFile, fs.getConf(), new ParquetGroupWriteSupport(schema, newMetadata));
                    ParquetReader<Object> dest = new ParquetReader(fs.getConf(), lastAvailableFinalPath, new ParquetGroupReadSupport(schema));
                    ParquetReader<Object> temp = new ParquetReader(fs.getConf(), lastAvailableFinalPath, new ParquetGroupReadSupport(schema))
                ) {
                    Object o;
                    while ((o = dest.read()) != null) {
                        writerPF.write(o);
                    }
                    while ((o = temp.read()) != null) {
                        writerPF.write(o);
                    }
                }

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
        try (ParquetFileReader pfr = ParquetFileReader.open(fs.getConf(), temporaryHdfsPath)) {
            MessageType schema2 = pfr.getFileMetaData().getSchema();

            return schema.equals(schema2);
        }
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
        double timestamp = getLatestCommittedTimestamp();
        PrometheusMetrics.latestCommittedTimestampGauge(eventName, partition).set(timestamp);
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
                try (ParquetFileReader pfr = ParquetFileReader.open(fs.getConf(), latestFileCommitted.get())) {
                    String timestamp = pfr
                        .getFooter()
                        .getFileMetaData()
                        .getKeyValueMetaData()
                        .getOrDefault(LATEST_TIMESTAMP_META_KEY, String.valueOf(defaultValue));
                    return Double.valueOf(timestamp);
                }
            } else {
                return defaultValue;
            }
        } catch (IOException e) {
            LOGGER.warn("could not get last existing final path. Defaulting latest committed timestamp to 0");
            return defaultValue;
        }
    }

    public ParquetWriter<MESSAGE_KIND> getWriter() {
        return writer;
    }

    public Path getFinalHdfsDir() {
        return finalHdfsDir;
    }

    public LocalDateTime getDayStartTime() {
        return dayStartTime;
    }

    public String getEventName() {
        return eventName;
    }


    public static class ParquetGroupReadSupport extends ReadSupport<Group> {

        private MessageType schema;

        ParquetGroupReadSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public ReadContext init(InitContext context) {
            return new ReadContext(schema);
        }

        @Override
        public RecordMaterializer<Group> prepareForRead(Configuration configuration,
                                                        Map<String, String> keyValueMetaData, MessageType fileSchema,
                                                        org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
            return new GroupRecordConverter(readContext.getRequestedSchema());
        }

    }

    public static class ParquetGroupWriteSupport extends WriteSupport<Group> {

        private MessageType schema;
        private GroupWriter groupWriter;
        private Map<String, String> extraMetaData;

        ParquetGroupWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
            this.schema = schema;
            this.extraMetaData = extraMetaData;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, this.extraMetaData);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            groupWriter = new GroupWriter(recordConsumer, schema);
        }

        @Override
        public void write(Group record) {
            groupWriter.write(record);
        }

    }
}
