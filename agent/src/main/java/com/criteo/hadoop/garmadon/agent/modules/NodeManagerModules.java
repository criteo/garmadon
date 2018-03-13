package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;

public class NodeManagerModules implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        new ContainerResourceMonitoringModule().setup(instrumentation, eventProcessor);
    }
}
