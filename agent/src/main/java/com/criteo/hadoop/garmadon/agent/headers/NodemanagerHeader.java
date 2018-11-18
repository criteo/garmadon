package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.schema.events.Header;

public class NodemanagerHeader {
    private Header header;

    private Header createCachedHeader() {
        return Header.newBuilder()
                .withHostname(Utils.getHostname())
                .withUser(Utils.getUser())
                .withPid(Utils.getPid())
                .addTag(Header.Tag.NODEMANAGER.name())
                .build();
    }

    private NodemanagerHeader() {
        this.header = createCachedHeader();
    }

    private static class SingletonHolder {
        private final static NodemanagerHeader INSTANCE = new NodemanagerHeader();
    }

    public static NodemanagerHeader getInstance() {
        return NodemanagerHeader.SingletonHolder.INSTANCE;
    }

    public Header getBaseHeader() {
        return header;
    }

}
