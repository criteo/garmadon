package com.criteo.hadoop.garmadon.agent.modules;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatcher;

abstract class MethodTracer extends Tracer {

    MethodTracer() {
        agentBuilder = agentBuilder
                .type(typeMatcher())
                .transform((builder, type, classLoader, module) ->
                        builder
                                .method(methodMatcher())
                                .intercept(newImplementation())
                );
    }


    abstract ElementMatcher<? super TypeDescription> typeMatcher();

    abstract ElementMatcher<? super MethodDescription> methodMatcher();

    abstract Implementation newImplementation();

}
