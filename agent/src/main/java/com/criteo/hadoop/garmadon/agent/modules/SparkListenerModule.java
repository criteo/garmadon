package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.schema.enums.Component;
import com.criteo.hadoop.garmadon.schema.enums.Framework;
import com.criteo.hadoop.garmadon.spark.listener.SparkListernerConf;

import java.lang.instrument.Instrumentation;
import java.util.Properties;
import java.util.function.Consumer;

public class SparkListenerModule extends ContainerModule {

    @Override
    public void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer) {
        if (this.getFramework().equals(Framework.SPARK) && this.getComponent().equals(Component.APP_MASTER)) {
            SparkListernerConf.getInstance().setConsumer(eventConsumer);
            Properties props = System.getProperties();
            props.setProperty("spark.extraListeners", "com.criteo.hadoop.garmadon.spark.listener.GarmadonSparkListener");
        }
    }
}
