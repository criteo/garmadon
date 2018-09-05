package com.criteo.jvm;

import java.util.function.Consumer;

public class LogStatisticCollector extends StatisticCollector<String> {
    public LogStatisticCollector(Consumer<String> printer) {
        super(printer, new StatisticsLog());
    }
}
