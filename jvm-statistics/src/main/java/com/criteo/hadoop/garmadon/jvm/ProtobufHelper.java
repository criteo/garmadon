package com.criteo.hadoop.garmadon.jvm;

public class ProtobufHelper {
    private static final String SINK_TYPE = "proto";

    protected ProtobufHelper() {
        throw new UnsupportedOperationException();
    }

    public static void install(Conf conf) {
        conf.setSinkType(SINK_TYPE);
        JVMStatistics.registerStatisticCollector(SINK_TYPE, ProtobufStatisticCollector.class);
        JVMStatistics.registerGCNotifications(SINK_TYPE, ProtobufGCNotifications.class);
    }
}
