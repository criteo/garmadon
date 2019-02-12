package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.schema.events.Header;

public final class RessourceManagerHeader {
    private Header header;

    private RessourceManagerHeader() {
        this.header = createCachedHeader();
    }

    private Header createCachedHeader() {
        return Header.newBuilder()
                .withHostname(Utils.getHostname())
                .withUser(Utils.getUser())
                .withPid(Utils.getPid())
                .addTag(Header.Tag.RESOURCEMANAGER.name())
                .addTags(System.getProperty("garmadon.tags"))
                .build();
    }

    private static class SingletonHolder {
        private final static RessourceManagerHeader INSTANCE = new RessourceManagerHeader();
    }

    public static RessourceManagerHeader getInstance() {
        return RessourceManagerHeader.SingletonHolder.INSTANCE;
    }

    public Header getBaseHeader() {
        return header;
    }

}
