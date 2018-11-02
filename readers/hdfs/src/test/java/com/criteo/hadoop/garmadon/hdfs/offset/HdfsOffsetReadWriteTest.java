package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;

import static com.criteo.hadoop.garmadon.hdfs.TestUtils.localDateTimeFromDate;

public class HdfsOffsetReadWriteTest {
    @Test
    public void test() throws IOException {
        java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");

        try {
            Path rootPath = new Path(tmpDir.toString());
            // Make sure we can read from subdirectories
            Path basePath = new Path(rootPath, "embedded");
            FileSystem localFs = FileSystem.getLocal(new Configuration());

            HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(
                    localFs, basePath, new TrailingPartitionOffsetPattern());

            TrailingPartitionOffsetFileNamer fileNamer = new TrailingPartitionOffsetFileNamer();
            LocalDateTime day1 = localDateTimeFromDate("1987-08-13 00:00:00");
            LocalDateTime day2 = localDateTimeFromDate("1987-08-14 00:00:00");

            /*
                /tmp/hdfs-reader-test-1234
                └── embedded
                    ├── 1987-08-13
                    │   ├── 1.2
                    │   ├── 1.3
                    │   └── 2.12
                    └── 1987-08-14
                        └── 1.1
             */
            localFs.mkdirs(rootPath);
            localFs.mkdirs(basePath);
            localFs.create(new Path(basePath, fileNamer.buildPath(day1, buildOffset(1, 2))));
            localFs.create(new Path(basePath, fileNamer.buildPath(day1, buildOffset(1, 3))));
            localFs.create(new Path(basePath, fileNamer.buildPath(day1, buildOffset(2, 12))));
            localFs.create(new Path(basePath, fileNamer.buildPath(day2, buildOffset(1, 1))));

            Assert.assertEquals(3, hdfsOffsetComputer.compute(1));
            Assert.assertEquals(12, hdfsOffsetComputer.compute(2));
            Assert.assertEquals(-1, hdfsOffsetComputer.compute(3));
        }
        finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }

    private Offset buildOffset(int partition, long offset) {
        return new TopicPartitionOffset("Dummy topic", partition, offset);
    }
}
