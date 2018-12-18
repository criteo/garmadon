package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.StandaloneHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer;
import com.criteo.hadoop.garmadon.agent.tracers.jvm.JVMStatisticsTracer;
import com.criteo.hadoop.garmadon.agent.tracers.presto.PluginClassLoaderTracer;

import java.lang.instrument.Instrumentation;

public class PrestoModule implements GarmadonAgentModule {

    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        // JVM/GC metrics/events
        JVMStatisticsTracer.setup((timestamp, event) ->
                eventProcessor.offer(timestamp, StandaloneHeader.getInstance().getHeader(), event));

        // Byte code instrumentation
        PluginClassLoaderTracer.setup(instrumentation);
        FileSystemTracer.setup(instrumentation, (timestamp, event) ->
                eventProcessor.offer(timestamp, StandaloneHeader.getInstance().getHeader(), event));
    }
}
