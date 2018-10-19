package com.criteo.hadoop.garmadon.agent.tracers;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatcher;

public abstract class MethodTracer extends Tracer {

    protected MethodTracer() {
        agentBuilder = agentBuilder
                .type(typeMatcher())
                .transform((builder, type, classLoader, module) ->
                        builder
                                .method(methodMatcher())
                                .intercept(newImplementation())
                );
    }


    protected abstract ElementMatcher<? super TypeDescription> typeMatcher();

    protected abstract ElementMatcher<? super MethodDescription> methodMatcher();

    protected abstract Implementation newImplementation();

}
