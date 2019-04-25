package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.RessourceManagerHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager.RMAppTracer;
import com.criteo.hadoop.garmadon.agent.tracers.jvm.JVMStatisticsTracer;

import java.lang.instrument.Instrumentation;

public class ResourceManagerModule implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        JVMStatisticsTracer.setup((timestamp, event) ->
            eventProcessor.offer(timestamp, RessourceManagerHeader.getInstance().getBaseHeader(), event));

        FileSystemTracer.setup(instrumentation,
            (timestamp, event) -> eventProcessor.offer(timestamp, RessourceManagerHeader.getInstance().getBaseHeader(), event));

        RMAppTracer.setup(RessourceManagerHeader.getInstance().getBaseHeader(),
                instrumentation, eventProcessor);
    }
}