package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.jvm.Conf;
import com.criteo.jvm.JVMStatistics;
import com.criteo.jvm.JVMStatisticsProtos;
import com.criteo.jvm.ProtobufHelper;

import java.lang.instrument.Instrumentation;
import java.time.Duration;
import java.util.function.Consumer;

public class JVMStatisticsModule extends ContainerModule {

    @Override
    public void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer) {
        Conf<JVMStatisticsProtos.JVMStatisticsData, JVMStatisticsProtos.GCStatisticsData, Void> conf = new Conf<>();
        int interval = Integer.getInteger("garmadon.jvm-statistics.interval", 10);
        conf.setInterval(Duration.ofSeconds(interval));
        conf.setLogJVMStats(eventConsumer::accept);
        conf.setLogGcStats(eventConsumer::accept);
        ProtobufHelper.install(conf);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
    }
}
