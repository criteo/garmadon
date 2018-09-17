package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.schema.events.Header;

import java.util.function.BiConsumer;

public class SparkListernerConf {

    private BiConsumer<Header, Object> eventHandler;
    private Header.SerializedHeader header;

    /**
     * Constructeur privé
     */
    private SparkListernerConf() {
    }

    /**
     * Holder
     */
    private static class SingletonHolder {
        /**
         * Instance unique non préinitialisée
         */
        private final static SparkListernerConf instance = new SparkListernerConf();
    }

    /**
     * Point d'accès pour l'instance unique du singleton
     */
    public static SparkListernerConf getInstance() {
        return SingletonHolder.instance;
    }

    public void setConsumer(BiConsumer<Header, Object> eventConsumer) {
        this.eventHandler = eventConsumer;
    }

    public Header.SerializedHeader getHeader() {
        return header;
    }

    public void setHeader(Header.SerializedHeader header) {
        this.header = header;
    }

    public BiConsumer<Header, Object> getEventHandler() {
        return eventHandler;
    }
}
