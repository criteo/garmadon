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

public class ParquetWithOffsetWriter<MessageKind extends MessageOrBuilder> implements CloseableBiConsumer<MessageKind, Offset> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetWithOffsetWriter.class);

    private final Path temporaryHdfsPath;
    private Offset latestOffset;
    private final ProtoParquetWriter<MessageKind> writer;
    private final Path finalHdfsDir;
    private FileSystem fs;
    private FileNamer fileNamer;
    private LocalDateTime dayStartTime;

    public ParquetWithOffsetWriter(ProtoParquetWriter<MessageKind> writer, Path temporaryHdfsPath, Path finalHdfsDir,
                                   FileSystem fs, FileNamer fileNamer, LocalDateTime dayStartTime) {
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
            String additionalInfo = String.format(" Date = %s, Temp file = %s",
                    dayStartTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), temporaryHdfsPath.toUri());
            LOGGER.error(String.format("Trying to write a zero-sized file, please fix (%s)", additionalInfo));
            return null;
        }

        writer.close();

        Path finalPath = new Path(finalHdfsDir, fileNamer.buildPath(dayStartTime, latestOffset));
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
