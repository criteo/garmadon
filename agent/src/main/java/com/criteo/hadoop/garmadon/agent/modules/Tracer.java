package com.criteo.hadoop.garmadon.agent.modules;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener.Filtering;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.NamedElement;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.*;

abstract class Tracer {

    protected AgentBuilder agentBuilder;

    private static ElementMatcher.Junction<NamedElement> ignoredPackagesMatcher = none();
    static {
        String[] ignoredPackages = {
                "com.cloudera.org.codehaus.jackson.",
                "com.codahale.metrics.",
                "com.criteo.hadoop.garmadon.",
                "com.criteo.jvm.",
                "com.esotericsoftware.",
                "com.fasterxml.",
                "com.google.",
                "com.sun.",
                "io.netty.",
                "java.",
                "javassist.",
                "javax.",
                "jdk.",
                "net.bytebuddy.",
                "net.jpountz.",
                "org.apache.commons.",
                "org.apache.hadoop.metrics2.",
                "org.apache.hadoop.yarn.proto.",
                "org.apache.htrace.",
                "org.apache.log4j.",
                "org.apache.spark.sql.",
                "org.apache.xerces.",
                "org.antlr.",
                "org.codehaus.",
                "org.glassfish.",
                "org.json4s.",
                "org.mockito.",
                "org.slf4j.",
                "org.spark_project.guava.",
                "org.spark_project.jetty.",
                "org.threeten.",
                "org.w3c.",
                "org.xml.",
                "oshi.",
                "scala.",
                "sun."
        };
        ignorePackages(ignoredPackages);

        String userIgnoredPackages = System.getProperty("bytebuddy.ignore.package.for.instrumentation");
        if(userIgnoredPackages != null) ignorePackages(userIgnoredPackages.split(","));
    }

    private static void ignorePackages(String[] ignoredPackages) {
        for (String ignoredPackage : ignoredPackages) {
            ignoredPackagesMatcher = ignoredPackagesMatcher.or(nameStartsWith(ignoredPackage));
        }
    }

    private static final ElementMatcher<? super String> byteBuddyLoggingFilter;
    static{
        String filterClass = System.getProperty("bytebuddy.debug.instrumentation.for.class");
        byteBuddyLoggingFilter = filterClass != null ? s -> s.contains(filterClass) : s -> false;
    }

    Tracer() {
        this.agentBuilder = new AgentBuilder.Default()
                .ignore(any(), isBootstrapClassLoader())
                .ignore(any(), isSystemClassLoader())
                .ignore(any(), isExtensionClassLoader())
                .ignore(ignoredPackagesMatcher)
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
