package com.criteo.jvm;

public class ProtobufHelper {
    private static final String SINK_TYPE = "proto";

    public static void install(Conf conf) {
        conf.setSinkType(SINK_TYPE);
        JVMStatistics.registerStatisticCollector(SINK_TYPE, ProtobufStatisticCollector.class);
        JVMStatistics.registerGCNotifications(SINK_TYPE, ProtobufGCNotifications.class);
    }
}
