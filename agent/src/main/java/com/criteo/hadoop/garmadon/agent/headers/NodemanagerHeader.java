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

    /**
     * Constructeur privé
     */
    private NodemanagerHeader() {
        this.header = createCachedHeader();
    }

    /**
     * Holder
     */
    private static class SingletonHolder {
        /**
         * Instance unique non préinitialisée
         */
        private final static NodemanagerHeader instance = new NodemanagerHeader();
    }

    /**
     * Point d'accès pour l'instance unique du singleton
     */
    public static NodemanagerHeader getInstance() {
        return NodemanagerHeader.SingletonHolder.instance;
    }

    public Header getBaseHeader() {
        return header;
    }

}
