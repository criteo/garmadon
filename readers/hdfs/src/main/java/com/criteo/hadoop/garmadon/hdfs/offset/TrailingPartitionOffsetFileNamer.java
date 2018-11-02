package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.FileNamer;
import com.criteo.hadoop.garmadon.reader.Offset;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Name files with _date_/_partition_._offset_
 */
public class TrailingPartitionOffsetFileNamer implements FileNamer {
    @Override
    public String buildPath(LocalDateTime dayStartTime, Offset offset) {
        return String.format("%s/%d.%d", dayStartTime.format(DateTimeFormatter.ISO_DATE), offset.getPartition(),
                offset.getOffset());
    }
}
