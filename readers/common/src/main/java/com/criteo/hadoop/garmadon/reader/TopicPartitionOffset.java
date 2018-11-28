package com.criteo.hadoop.garmadon.reader;

public class TopicPartitionOffset implements Offset {
    private final String topic;
    private final int partition;
    private final long offset;

    public TopicPartitionOffset(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}