package com.criteo.hadoop.garmadon.agent.tracers;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatcher;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static net.bytebuddy.matcher.ElementMatchers.any;

public abstract class ConstructorTracer extends Tracer {

    protected ConstructorTracer() {
        agentBuilder = agentBuilder
                .type(typeMatcher())
                .transform((builder, type, classLoader, module) ->
                        builder
                                .constructor(constructorMatcher())
                                .intercept(newImplementation()));
    }


    protected abstract ElementMatcher<? super TypeDescription> typeMatcher();

    protected abstract ElementMatcher<? super MethodDescription> constructorMatcher();

    protected abstract Implementation newImplementation();

}
