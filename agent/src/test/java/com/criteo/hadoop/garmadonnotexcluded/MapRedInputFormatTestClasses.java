package com.criteo.hadoop.garmadonnotexcluded;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class MapRedInputFormatTestClasses {

    public static class SimpleInputFormat implements InputFormat{
        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            return new InputSplit[0];
        }

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return null;
        }
    };

    public static class OneLevelHierarchy implements InputFormat {

        public static RecordReader recordReaderMock = mock(RecordReader.class);

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return recordReaderMock;
        }
    }

    public static abstract class AbstractInputFormat implements InputFormat {

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        abstract public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter);
    }

    public static class RealInputFormat extends AbstractInputFormat {

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) {
            return null;
        }
    }

    public static class Level1 implements InputFormat {

        static RecordReader recordReaderMock = mock(RecordReader.class);

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return recordReaderMock;
        }
    }

    public static class Level2CallingSuper extends Level1 {

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            System.out.println("Do something before");
            RecordReader rr = super.getRecordReader(inputSplit, jobConf, reporter);
            System.out.println("Do something after");
            return rr;
        }
    }

    public static class Level3NotCallingSuper extends Level2CallingSuper {

        static RecordReader recordReaderMock = mock(RecordReader.class);
        public static boolean isAccessed = false;

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            isAccessed = true;
            return recordReaderMock;
        }
    }

}
