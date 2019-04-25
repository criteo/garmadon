package com.criteo.hadoop.garmadon.flink;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;

public final class GarmadonFlinkConf {

    private TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;

    private GarmadonFlinkConf() {
    }

    private static class SingletonHolder {
        private final static GarmadonFlinkConf INSTANCE = new GarmadonFlinkConf();
    }

    public static GarmadonFlinkConf getInstance() {
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
