package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;

public final class SparkListernerConf {

    private TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;

    private SparkListernerConf() {
    }

    private static class SingletonHolder {
        private final static SparkListernerConf INSTANCE = new SparkListernerConf();
    }

    public static SparkListernerConf getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public void setConsumer(TriConsumer<Long, Header, Object> eventConsumer) {
        this.eventHandler = eventConsumer;
    }

    public Header.SerializedHeader getHeader() {
        return header;
    }

    public void setHeader(Header.SerializedHeader header) {
        this.header = header;
    }

    public TriConsumer<Long, Header, Object> getEventHandler() {
        return eventHandler;
    }
}
