package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

/**
 * Wrap an actual ProtoParquetWriter, renaming the output file properly when closing.
 *
 * @param <MESSAGE_KIND>     The message to be written in Proto + Parquet
 */
public class ProtoParquetWriterWithOffset<MESSAGE_KIND extends MessageOrBuilder>
        implements CloseableBiConsumer<MESSAGE_KIND, Offset> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoParquetWriterWithOffset.class);

    private final Path temporaryHdfsPath;
    private final ProtoParquetWriter<MESSAGE_KIND> writer;
    private final Path finalHdfsDir;
    private final FileSystem fs;
    private final OffsetComputer fileNamer;
    private final LocalDateTime dayStartTime;

    private Offset latestOffset;

    /**
     * @param writer            The actual Proto + Parquet writer
     * @param temporaryHdfsPath The path to which the writer will output events
     * @param finalHdfsDir      The directory to write the final output to (renamed from temporaryHdfsPath)
     * @param fs                The filesystem on which both the temporary and final files reside
     * @param fileNamer         File-naming logic for the final path
     * @param dayStartTime      The day partition the final file will go to
     */
    public ProtoParquetWriterWithOffset(ProtoParquetWriter<MESSAGE_KIND> writer, Path temporaryHdfsPath,
                                        Path finalHdfsDir, FileSystem fs, OffsetComputer fileNamer,
                                        LocalDateTime dayStartTime) {
        this.writer = writer;
        this.temporaryHdfsPath = temporaryHdfsPath;
        this.finalHdfsDir = finalHdfsDir;
        this.fs = fs;
        this.fileNamer = fileNamer;
        this.dayStartTime = dayStartTime;
        this.latestOffset = null;
    }

    @Override
    public Path close() throws IOException {
        if (latestOffset == null) {
            final String additionalInfo = String.format(" Date = %s, Temp file = %s",
                    dayStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), temporaryHdfsPath.toUri());
            LOGGER.error("Trying to write a zero-sized file, please fix ({})", additionalInfo);
            return null;
        }

        writer.close();

        final Path finalPath = new Path(finalHdfsDir, fileNamer.computePath(dayStartTime, latestOffset));

        if (!FileSystemUtils.ensureDirectoriesExist(Collections.singleton(finalPath.getParent()), fs)) {
            LOGGER.warn("Couldn't ensure {} exists", finalPath.getParent());
            return null;
        }

        if (!fs.rename(temporaryHdfsPath, finalPath)) {
            LOGGER.warn("Failed to commit {} (from {})", finalPath.toUri(), temporaryHdfsPath);
            return null;
        }

        LOGGER.info("Committed {} (from {})", finalPath.toUri(), temporaryHdfsPath);

        return finalPath;
    }

    @Override
    public void write(MESSAGE_KIND msg, Offset offset) throws IOException {
        if (latestOffset == null || offset.getOffset() > latestOffset.getOffset()) latestOffset = offset;

        if (msg != null) writer.write(msg);
    }
}
