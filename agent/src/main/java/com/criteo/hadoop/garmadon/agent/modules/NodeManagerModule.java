package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.NodemanagerHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.nodemanager.ContainerResourceMonitoringTracer;

import java.lang.instrument.Instrumentation;

public class NodeManagerModule implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        ContainerResourceMonitoringTracer.setup(NodemanagerHeader.getInstance().getBaseHeader(),
                instrumentation, eventProcessor);
    }
}