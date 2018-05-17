package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.*;

public class ContainerModules implements GarmadonAgentModule {

    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        executorService.submit(() -> new JVMStatisticsModule().setup(instrumentation, eventProcessor));
        executorService.submit(() -> {
            new FileSystemModule().setup(instrumentation, eventProcessor);
            new MapReduceModule().setup(instrumentation, eventProcessor);
        });
        executorService.shutdown();
        // We wait 3 sec executor to instrument classes
        // If all classes are still not instrumented after that time we let the JVM continue startup
        // in order to not block the container for too long on agent initialization
        // Currently we are seeing avg duration of 160 s on MapRed and 6500 s on Spark
        // so we consider 3s as a reasonable duration
        // Downside: we can have some classes not instrumenting loaded by the JVM so missing
        // some metrics on a container
        try {
            executorService.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }
}
