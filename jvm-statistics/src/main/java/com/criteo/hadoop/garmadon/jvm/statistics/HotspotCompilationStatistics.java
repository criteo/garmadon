package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import sun.management.HotspotCompilationMBean;

import java.lang.management.CompilationMXBean;

class HotspotCompilationStatistics extends CompilationStatistics {
    private static final String COMPILE_VAR_COUNT = "count";
    private static final String COMPILE_VAR_INVALIDATED = "invalidated";
    private static final String COMPILE_VAR_FAILED = "failed";
    private static final String COMPILE_VAR_THREADS = "threads";

    private final HotspotCompilationMBean hsCompilation;

    HotspotCompilationStatistics(CompilationMXBean compilation, HotspotCompilationMBean hsCompilation) {
        super(compilation);
        this.hsCompilation = hsCompilation;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.addDuration(COMPILE_VAR_COUNT, hsCompilation.getTotalCompileCount());
        super.innerCollect(sink);
        sink.addDuration(COMPILE_VAR_INVALIDATED, hsCompilation.getInvalidatedCompileCount());
        sink.addDuration(COMPILE_VAR_FAILED, hsCompilation.getBailoutCompileCount());
        sink.addDuration(COMPILE_VAR_THREADS, hsCompilation.getCompilerThreadCount());
    }
}
