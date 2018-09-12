package com.criteo.hadoop.garmadon.agent.tracers;

import com.criteo.hadoop.garmadon.spark.listener.SparkListernerConf;

import java.lang.instrument.Instrumentation;
import java.util.Properties;
import java.util.function.Consumer;

public class SparkListenerTracer {
    public static void setup(Consumer<Object> eventConsumer) {
        SparkListernerConf.getInstance().setConsumer(eventConsumer);
        Properties props = System.getProperties();
        props.setProperty("spark.extraListeners", "com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkListener");
    }
}
