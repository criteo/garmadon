package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.events.HeaderUtils;

public final class NodemanagerHeader {
    private Header header;

    private NodemanagerHeader() {
        this.header = createCachedHeader();
    }

    private Header createCachedHeader() {
        return Header.newBuilder()
            .withHostname(HeaderUtils.getHostname())
            .withUser(HeaderUtils.getUser())
            .withPid(HeaderUtils.getPid())
            .withMainClass(HeaderUtils.getJavaMainClass())
            .addTag(Header.Tag.NODEMANAGER.name())
            .addTags(System.getProperty("garmadon.tags"))
            .build();
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
