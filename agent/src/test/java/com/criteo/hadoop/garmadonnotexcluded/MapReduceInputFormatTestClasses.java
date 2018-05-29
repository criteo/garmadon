package com.criteo.hadoop.garmadonnotexcluded;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;

public class MapReduceInputFormatTestClasses {

    public static class OneLevelHierarchy extends InputFormat {

        public static List<InputSplit> listInputSplits = mock(List.class);
        public static RecordReader recordReaderMock = mock(RecordReader.class);

        @Override
        public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
            return listInputSplits;
        }

        @Override
        public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            return recordReaderMock;
        }
    }
}
