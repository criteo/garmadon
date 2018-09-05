package com.criteo.jvm;

import java.util.function.Consumer;

public class ProtobufStatisticCollector extends StatisticCollector<JVMStatisticsProtos.JVMStatisticsData> {

    public ProtobufStatisticCollector(Consumer<JVMStatisticsProtos.JVMStatisticsData> printer) {
        super(printer, new ProtobufStatisticsSink());
    }
}
