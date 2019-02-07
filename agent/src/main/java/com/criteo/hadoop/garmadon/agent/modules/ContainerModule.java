package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.headers.ContainerHeader;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer;
import com.criteo.hadoop.garmadon.agent.tracers.jvm.JVMStatisticsTracer;
import com.criteo.hadoop.garmadon.agent.tracers.spark.SparkListenerTracer;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ContainerModule implements GarmadonAgentModule {

    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        // JVM/GC metrics/events
        executorService.submit(() -> JVMStatisticsTracer.setup((timestamp, event) ->
                eventProcessor.offer(timestamp, ContainerHeader.getInstance().getHeader(), event)));

        // Byte code instrumentation
        executorService.submit(() -> FileSystemTracer.setup(instrumentation,
                (timestamp, event) -> eventProcessor.offer(timestamp, ContainerHeader.getInstance().getHeader(), event)));

        // Set SPARK Listener
        executorService.submit(() -> {
            SparkListenerTracer.setup(ContainerHeader.getInstance().getHeader(),
                    (timestamp, header, event) -> eventProcessor.offer(timestamp, header, event));
        });

        executorService.shutdown();
        // We wait 3 sec executor to instrument classes
        // If all classes are still not instrumented after that time we let the JVM continue startup
        // in order to not block the container for too long on agent initialization
        // Currently we are seeing avg duration of 160 s on MapRed and 6500 s on SPARK
        // so we consider 3s as a reasonable duration
        // Downside: we can have some classes not instrumenting loaded by the JVM so missing
        // some metrics on a container
        try {
            executorService.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }
}
