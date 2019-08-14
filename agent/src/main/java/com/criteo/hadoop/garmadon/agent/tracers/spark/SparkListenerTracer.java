package com.criteo.hadoop.garmadon.agent.tracers.spark;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkListener;
import com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkStorageStatusListener;
import com.criteo.hadoop.garmadon.spark.listener.SparkListernerConf;

import java.util.Properties;

public class SparkListenerTracer {

    protected SparkListenerTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header.SerializedHeader header, TriConsumer<Long, Header, Object> eventConsumer) {
        SparkListernerConf sparkListernerConf = SparkListernerConf.getInstance();
        sparkListernerConf.setConsumer(eventConsumer);
        sparkListernerConf.setHeader(header);
        Properties props = System.getProperties();
        props.setProperty("spark.extraListeners", GarmadonSparkListener.class.getName() + "," + GarmadonSparkStorageStatusListener.class.getName());
    }
}
