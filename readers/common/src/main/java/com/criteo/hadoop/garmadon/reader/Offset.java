package com.criteo.hadoop.garmadon.reader;

public interface Offset {

    String getTopic();

    int getPartition();

    long getOffset();
}
