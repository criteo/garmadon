package com.criteo.hadoop.garmadon.agent.tracers;

import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.spark.listener.SparkListernerConf;

import java.util.Properties;
import java.util.function.BiConsumer;

public class SparkListenerTracer {
    public static void setup(Header.SerializedHeader header, BiConsumer<Header, Object> eventConsumer) {
        SparkListernerConf sparkListernerConf = SparkListernerConf.getInstance();
        sparkListernerConf.setConsumer(eventConsumer);
        sparkListernerConf.setHeader(header);
        Properties props = System.getProperties();
        props.setProperty("spark.extraListeners", "com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkListener");
    }
}
