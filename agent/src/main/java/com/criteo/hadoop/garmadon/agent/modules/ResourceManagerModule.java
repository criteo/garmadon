package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.RessourceManagerHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager.RMAppTracer;

import java.lang.instrument.Instrumentation;

public class ResourceManagerModule implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        RMAppTracer.setup(RessourceManagerHeader.getInstance().getBaseHeader(),
                instrumentation, eventProcessor);
    }
}