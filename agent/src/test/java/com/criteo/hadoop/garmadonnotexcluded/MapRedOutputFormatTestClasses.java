package com.criteo.hadoop.garmadonnotexcluded;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class MapRedOutputFormatTestClasses {

    public static class OneLevelHierarchy implements OutputFormat {

        public static RecordWriter recordWriterMock = mock(RecordWriter.class);

        @Override
        public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
            return recordWriterMock;
        }

        @Override
        public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }
    }
}
