package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.FileNamer;
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

/**
 * Wrap an actual ProtoParquetWriter, renaming the output file properly when closing.
 *
 * @param <MessageKind>     The message to be written in Proto + Parquet
 */
public class ProtoParquetWriterWithOffset<MessageKind extends MessageOrBuilder>
        implements CloseableBiConsumer<MessageKind, Offset> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoParquetWriterWithOffset.class);

    private final Path temporaryHdfsPath;
    private final ProtoParquetWriter<MessageKind> writer;
    private final Path finalHdfsDir;
    private final FileSystem fs;
    private final FileNamer fileNamer;
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
    public ProtoParquetWriterWithOffset(ProtoParquetWriter<MessageKind> writer, Path temporaryHdfsPath,
                                        Path finalHdfsDir, FileSystem fs, FileNamer fileNamer,
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

        final Path finalPath = new Path(finalHdfsDir, fileNamer.buildPath(dayStartTime, latestOffset));
        fs.rename(temporaryHdfsPath, finalPath);

        LOGGER.info("Committed " + finalPath.toUri());

        return finalPath;
    }

    @Override
    public void write(MessageKind msg, Offset offset) throws IOException {
        if (latestOffset == null || offset.getOffset() > latestOffset.getOffset())
            latestOffset = offset;

        if (msg != null)
            writer.write(msg);
    }
}
