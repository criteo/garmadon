package com.criteo.hadoop.garmadonnotexcluded;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class MapReduceOutputFormatTestClasses {

    public static class OneLevelHierarchy extends OutputFormat {

        public static RecordWriter recordWriterMock = mock(RecordWriter.class);

        @Override
        public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return recordWriterMock;
        }

        @Override
        public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
            throw new RuntimeException("not supposed to be used");
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            throw new RuntimeException("not supposed to be used");
        }
    }

}
