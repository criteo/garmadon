package com.criteo.hadoop.garmadon.agent.tracers.jvm;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.jvm.Conf;
import com.criteo.jvm.JVMStatistics;
import com.criteo.jvm.ProtobufHelper;

import java.time.Duration;
import java.util.function.BiConsumer;

public class JVMStatisticsTracer {

    public static void setup(BiConsumer<Long, Object> eventConsumer) {
        Conf<JVMStatisticsEventsProtos.JVMStatisticsData, JVMStatisticsEventsProtos.GCStatisticsData, Void> conf = new Conf<>();
        int interval = Integer.getInteger("garmadon.jvm-statistics.interval", 10);
        conf.setInterval(Duration.ofSeconds(interval));
        conf.setLogJVMStats(eventConsumer::accept);
        conf.setLogGcStats(eventConsumer::accept);
        ProtobufHelper.install(conf);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
    }
}
