package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;

import java.lang.instrument.Instrumentation;

public interface GarmadonAgentModule {
    void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor);
}
