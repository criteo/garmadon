package com.criteo.hadoop.garmadon.agent.tracers.flink;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.flink.GarmadonFlinkConf;
import com.criteo.hadoop.garmadon.schema.events.Header;

import java.util.Properties;

public class FlinkReporterTracer {

    protected FlinkReporterTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header.SerializedHeader header, TriConsumer<Long, Header, Object> eventConsumer) {
        GarmadonFlinkConf flinkConf = GarmadonFlinkConf.getInstance();
        flinkConf.setConsumer(eventConsumer);
        flinkConf.setHeader(header);
        Properties props = System.getProperties();
        props.setProperty("metrics.reporters", "garmadon");
        props.setProperty("metrics.reporter.garmadon.class", "com.criteo.hadoop.garmadon.flink.GarmadonFlinkReporter");
    }
}
