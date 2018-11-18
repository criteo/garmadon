package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;

public class SparkListernerConf {

    private TriConsumer<Long, Header, Object> eventHandler;
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
        private final static SparkListernerConf INSTANCE = new SparkListernerConf();
    }

    /**
     * Point d'accès pour l'INSTANCE unique du singleton
     */
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
