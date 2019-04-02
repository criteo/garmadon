package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.NodemanagerHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.nodemanager.ContainerResourceMonitoringTracer;
import com.criteo.hadoop.garmadon.agent.tracers.jvm.JVMStatisticsTracer;

import java.lang.instrument.Instrumentation;

public class NodeManagerModule implements GarmadonAgentModule {
    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        JVMStatisticsTracer.setup((timestamp, event) ->
            eventProcessor.offer(timestamp, NodemanagerHeader.getInstance().getBaseHeader(), event));

        FileSystemTracer.setup(instrumentation,
            (timestamp, event) -> eventProcessor.offer(timestamp, NodemanagerHeader.getInstance().getBaseHeader(), event));

        ContainerResourceMonitoringTracer.setup(NodemanagerHeader.getInstance().getBaseHeader(),
                instrumentation, eventProcessor);
    }
}