package com.criteo.hadoop.garmadon.agent.tracers.spark;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkListener;
import com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkStorageStatusListener;
import com.criteo.hadoop.garmadon.spark.listener.SparkListernerConf;

import java.util.Properties;

public class SparkListenerTracer {
    private static final String SPARK_EXTRA_LISTENERS_KEY = "spark.extraListeners";

    protected SparkListenerTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header.SerializedHeader header, TriConsumer<Long, Header, Object> eventConsumer) {
        SparkListernerConf sparkListernerConf = SparkListernerConf.getInstance();
        sparkListernerConf.setConsumer(eventConsumer);
        sparkListernerConf.setHeader(header);
        Properties props = System.getProperties();

        StringBuilder newListeners = new StringBuilder();
        newListeners.append(GarmadonSparkListener.class.getName());
        newListeners.append(',');
        newListeners.append(GarmadonSparkStorageStatusListener.class.getName());
        if (props.containsKey(SPARK_EXTRA_LISTENERS_KEY)) {
            newListeners.append(',');
            newListeners.append(props.getProperty(SPARK_EXTRA_LISTENERS_KEY));
        }

        props.setProperty(SPARK_EXTRA_LISTENERS_KEY, newListeners.toString());
    }
}
