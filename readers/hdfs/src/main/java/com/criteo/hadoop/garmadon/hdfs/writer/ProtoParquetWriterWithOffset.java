package com.criteo.hadoop.garmadon.hdfs.writer;

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
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Wrap an actual ProtoParquetWriter, renaming the output file properly when closing.
 *
 * @param <MESSAGE_KIND> The message to be written in Proto + Parquet
 */
public class ProtoParquetWriterWithOffset<MESSAGE_KIND extends MessageOrBuilder>
    implements CloseableBiConsumer<MESSAGE_KIND, Offset> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoParquetWriterWithOffset.class);
    private static final Map<String, String> EMPTY_METADATA = new HashMap<>();

    private final Path temporaryHdfsPath;
    private final ProtoParquetWriter<MESSAGE_KIND> writer;
    private final Path finalHdfsDir;
    private final FileSystem fs;
    private final OffsetComputer fileNamer;
    private final LocalDateTime dayStartTime;
    private final String eventName;
    private final long fsBlockSize;

    private Offset latestOffset = null;
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
    public ProtoParquetWriterWithOffset(ProtoParquetWriter<MESSAGE_KIND> writer, Path temporaryHdfsPath,
                                        Path finalHdfsDir, FileSystem fs, OffsetComputer fileNamer,
                                        LocalDateTime dayStartTime, String eventName) {
        this.writer = writer;
        this.temporaryHdfsPath = temporaryHdfsPath;
        this.finalHdfsDir = finalHdfsDir;
        this.fs = fs;
        this.fileNamer = fileNamer;
        this.dayStartTime = dayStartTime;
        this.eventName = eventName;
        this.fsBlockSize = fs.getDefaultBlockSize(finalHdfsDir);
    }

    @Override
    public Path close() throws IOException {
        if (latestOffset == null) {
            final String additionalInfo = String.format(" Date = %s, Temp file = %s",
                dayStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), temporaryHdfsPath.toUri());
            throw new IOException(String.format("Trying to write a zero-sized file, please fix (%s)", additionalInfo));
        }

        PrometheusMetrics.buildGaugeChild(PrometheusMetrics.LATEST_COMMITTED_OFFSETS,
            eventName, latestOffset.getPartition()).set(latestOffset.getOffset());

        if (!writerClosed) {
            writer.close();
            writerClosed = true;
        }

        final Path topicGlobPath = new Path(finalHdfsDir, fileNamer.computeTopicGlob(dayStartTime, latestOffset));

        final Optional<Path> lastAvailableFinalPath = Arrays.stream(fs.globStatus(topicGlobPath))
            .map(FileStatus::getPath)
            .max(Comparator.comparingLong(path -> fileNamer.getIndex(path.getName())));

        long lastIndex = lastAvailableFinalPath.map(path -> fileNamer.getIndex(path.getName()) + 1).orElse(1L);

        final Path finalPath = new Path(finalHdfsDir, fileNamer.computePath(dayStartTime, lastIndex, latestOffset));

        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(finalPath.getParent()), fs);

        if (lastAvailableFinalPath.isPresent()) {
            long blockSize = fs.getFileStatus(lastAvailableFinalPath.get()).getLen();
            if (blockSize > fsBlockSize) {
                moveToFinalPath(temporaryHdfsPath, finalPath);
            } else {
                mergeToFinalPath(lastAvailableFinalPath.get(), finalPath);
            }
        } else {
            moveToFinalPath(temporaryHdfsPath, finalPath);
        }

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

        LOGGER.info("Committed {} to {}", temporaryHdfsPath, finalPath.toUri());
    }

    protected void mergeToFinalPath(Path lastAvailableFinalPath, Path finalPath) throws IOException {
        MessageType schema = ParquetFileReader.open(fs.getConf(), lastAvailableFinalPath).getFileMetaData().getSchema();
        if (!checkSchemaEquality(schema)) {
            LOGGER.warn("Schema between last available final file ({}) and temp file ({}) are not identical. We can't merge them",
                lastAvailableFinalPath, temporaryHdfsPath);
            moveToFinalPath(temporaryHdfsPath, finalPath);
        } else {
            Path mergedTempFile = new Path(temporaryHdfsPath.toString() + ".merged");

            if (fs.isFile(mergedTempFile)) fs.delete(mergedTempFile, false);

            ParquetFileWriter writerPF = new ParquetFileWriter(fs.getConf(), schema, mergedTempFile);
            writerPF.start();
            writerPF.appendFile(fs.getConf(), lastAvailableFinalPath);
            writerPF.appendFile(fs.getConf(), temporaryHdfsPath);
            writerPF.end(EMPTY_METADATA);

            moveToFinalPath(mergedTempFile, lastAvailableFinalPath);
            try {
                fs.delete(temporaryHdfsPath, false);
                // This file is in a temp folder that should be deleted at exit so we should not throw exception here
            } catch (IOException ignored) {
            }
        }
    }

    private boolean checkSchemaEquality(MessageType schema) throws IOException {
        MessageType schema2 = ParquetFileReader.open(fs.getConf(), temporaryHdfsPath).getFileMetaData().getSchema();

        return schema.equals(schema2);
    }


    @Override
    public void write(MESSAGE_KIND msg, Offset offset) throws IOException {
        if (latestOffset == null || offset.getOffset() > latestOffset.getOffset()) latestOffset = offset;

        if (msg != null) {
            writer.write(msg);
        }
    }
}
