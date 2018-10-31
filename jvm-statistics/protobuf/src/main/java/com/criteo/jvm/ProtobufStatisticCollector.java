package com.criteo.jvm;

import java.util.function.BiConsumer;

public class ProtobufStatisticCollector extends StatisticCollector<JVMStatisticsProtos.JVMStatisticsData> {

    public ProtobufStatisticCollector(BiConsumer<Long, JVMStatisticsProtos.JVMStatisticsData> printer) {
        super(printer, new ProtobufStatisticsSink());
    }
}
