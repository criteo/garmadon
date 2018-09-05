package com.criteo.jvm.statistics;

import com.criteo.jvm.AbstractStatistic;
import com.criteo.jvm.StatisticsSink;

import java.lang.management.CompilationMXBean;

class CompilationStatistics extends AbstractStatistic {
    private static final String COMPILE_HEADER = "compile";
    private static final String COMPILE_VAR_TIME = "time";

    private final CompilationMXBean compilation;

    CompilationStatistics(CompilationMXBean compilation) {
        super(COMPILE_HEADER);
        this.compilation = compilation;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.addDuration(COMPILE_VAR_TIME, compilation.getTotalCompilationTime());
    }
}

