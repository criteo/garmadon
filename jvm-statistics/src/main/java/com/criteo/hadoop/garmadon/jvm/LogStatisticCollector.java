package com.criteo.hadoop.garmadon.jvm;

import java.util.function.BiConsumer;

public class LogStatisticCollector extends StatisticCollector<String> {
    public LogStatisticCollector(BiConsumer<Long, String> printer) {
        super(printer, new StatisticsLog());
    }
}
