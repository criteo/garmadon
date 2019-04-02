package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.events.HeaderUtils;

public final class StandaloneHeader {
    private Header.SerializedHeader header;

    private StandaloneHeader() {
        this.header = createCachedHeader();
    }

    private Header.SerializedHeader createCachedHeader() {
        //build the header for the whole application once
        return Header.newBuilder()
                .withId(HeaderUtils.getStandaloneId())
                .addTag(Header.Tag.STANDALONE.name())
                .addTags(System.getProperty("garmadon.tags"))
                .withHostname(HeaderUtils.getHostname())
                .withUser(HeaderUtils.getUser())
                .withPid(HeaderUtils.getPid())
                .withMainClass(HeaderUtils.getJavaMainClass())
                .buildSerializedHeader();
    }

    private static class SingletonHolder {
        private final static StandaloneHeader INSTANCE = new StandaloneHeader();
    }

    public static StandaloneHeader getInstance() {
        return StandaloneHeader.SingletonHolder.INSTANCE;
    }

    public Header.SerializedHeader getHeader() {
        return header;
    }

}
