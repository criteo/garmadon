package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;
import java.util.function.Consumer;

/**
 * Abstract class for all modules related to containers
 * It's main purpose is to provide the header as raw bytes
 * Since we can get all information about the container
 * at the beginning, we gain serialization time
 */
public abstract class ContainerModule implements GarmadonAgentModule {

    protected ContainerModuleHeader containerModuleHeader;

    public ContainerModule() {
        containerModuleHeader = ContainerModuleHeader.getInstance();
    }


    @Override
    public final void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        setup0(instrumentation, event -> eventProcessor.offer(containerModuleHeader.getHeader(), event));
    }

    protected abstract void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer);
}
