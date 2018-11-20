package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.jvm.Conf;
import com.criteo.jvm.JVMStatistics;
import com.criteo.jvm.ProtobufHelper;

import java.time.Duration;

public class HostStatistics {

    private static JVMStatistics jvmStatistics;

    public static void startReport(ForwarderEventSender forwarderEventSender) {
        Conf<JVMStatisticsEventsProtos.JVMStatisticsData, Void, JVMStatisticsEventsProtos.JVMStatisticsData> conf = new Conf<>();
        int interval = Integer.getInteger("garmadon.machine-statistics.interval", 30);
        conf.setInterval(Duration.ofSeconds(interval));

        conf.setLogJVMStats(forwarderEventSender::sendAsync);
        conf.setLogMachineStats(forwarderEventSender::sendAsync);

        ProtobufHelper.install(conf);

        jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
    }

    public static void stopReport() {
        if (jvmStatistics != null) {
            jvmStatistics.stop();
        }
    }

}
