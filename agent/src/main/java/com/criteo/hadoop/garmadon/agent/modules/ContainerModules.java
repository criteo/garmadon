package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;

public class ContainerModules implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        new JVMStatisticsModule().setup(instrumentation, eventProcessor);
        new FileSystemModule().setup(instrumentation, eventProcessor);
        new MapReduceModule().setup(instrumentation, eventProcessor);
    }
}
