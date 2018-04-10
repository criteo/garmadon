package com.criteo.hadoop.garmadon.spark.listener;

import java.util.function.Consumer;

public class SparkListernerConf {

    private Consumer<Object> eventHandler;

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

    public void setConsumer(Consumer<Object> eventConsumer) {
        this.eventHandler = eventConsumer;
    }

    public Consumer<Object> getEventHandler() {
        return eventHandler;
    }
}
