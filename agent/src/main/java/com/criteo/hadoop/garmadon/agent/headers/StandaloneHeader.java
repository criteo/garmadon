package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.schema.events.Header;

public class StandaloneHeader {
    private Header.SerializedHeader header;

    private Header.SerializedHeader createCachedHeader() {
        //build the header for the whole application once
        return Header.newBuilder()
                .withId(Utils.getStandaloneId())
                .addTag(Header.Tag.STANDALONE.name())
                .withHostname(Utils.getHostname())
                .withUser(Utils.getUser())
                .withPid(Utils.getPid())
                .withMainClass(Utils.getArrayJavaCommandLine()[0])
                .buildSerializedHeader();
    }

    private StandaloneHeader() {
        this.header = createCachedHeader();
    }

    private static class SingletonHolder {
        private final static StandaloneHeader instance = new StandaloneHeader();
    }

    public static StandaloneHeader getInstance() {
        return StandaloneHeader.SingletonHolder.instance;
    }

    public Header.SerializedHeader getHeader() {
        return header;
    }

}
