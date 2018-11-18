package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.function.BiConsumer;

public class ProtobufStatisticCollector extends StatisticCollector<JVMStatisticsEventsProtos.JVMStatisticsData> {

    public ProtobufStatisticCollector(BiConsumer<Long, JVMStatisticsEventsProtos.JVMStatisticsData> printer) {
        super(printer, new ProtobufStatisticsSink());
    }
}
