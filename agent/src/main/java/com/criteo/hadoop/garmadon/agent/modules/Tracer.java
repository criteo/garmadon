package com.criteo.hadoop.garmadon.agent.modules;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener.Filtering;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;

abstract class Tracer {

    AgentBuilder agentBuilder;

    Tracer() {
        String filterClass = System.getProperty("bytebuddy.debug.instrumentation.for.class");
        ElementMatcher<? super String> byteBuddyLoggingFilter = filterClass != null ? s -> s.contains(filterClass) : s -> false;
        this.agentBuilder = new AgentBuilder.Default()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(
                        new Filtering(byteBuddyLoggingFilter, AgentBuilder.Listener.StreamWriting.toSystemOut())
                );
    }

    void installOn(Instrumentation instrumentation) {
        this.agentBuilder.installOn(instrumentation);
    }

    ResettableClassFileTransformer installOnByteBuddyAgent() {
        return this.agentBuilder.installOnByteBuddyAgent();
    }

}
