package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.*;

public class ContainerModules implements GarmadonAgentModule {

    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        executorService.submit(() -> new JVMStatisticsModule().setup(instrumentation, eventProcessor));
        executorService.submit(() -> new FileSystemModule().setup(instrumentation, eventProcessor));
        executorService.submit(() -> new MapReduceModule().setup(instrumentation, eventProcessor));
        executorService.shutdown();
    }
}
