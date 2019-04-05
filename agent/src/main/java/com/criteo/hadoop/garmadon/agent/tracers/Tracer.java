package com.criteo.hadoop.garmadon.agent.tracers;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener.Filtering;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.NamedElement;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.*;

public abstract class Tracer {

    protected static ElementMatcher.Junction<NamedElement> ignoredMatcher;

    private static final ElementMatcher<? super String> BYTE_BUDDY_LOGGING_FILTER;

    protected AgentBuilder agentBuilder;

    Tracer() {
        this.agentBuilder = new AgentBuilder.Default()
            .ignore(any(), isBootstrapClassLoader())
            .ignore(any(), isSystemClassLoader())
            .ignore(any(), isExtensionClassLoader())
            .ignore(ignoredMatcher)
            .with(
                new Filtering(BYTE_BUDDY_LOGGING_FILTER, AgentBuilder.Listener.StreamWriting.toSystemOut())
            );
    }

    static {
        String[] predefWhitelist = {
            "org.apache.flink.",
            "org.apache.hadoop.",
            "org.apache.spark.",
            "com.facebook.presto.",
            "io.prestosql.",
            "com.criteo.",
        };
        String[] predefBlacklist = {
            "com.criteo.hadoop.garmadon.",
            "com.criteo.jvm.",
        };
        String runtimeWhitelist = System.getProperty("bytebuddy.whitelist.for.instrumentation");
        String runtimeBlacklist = System.getProperty("bytebuddy.blacklist.for.instrumentation");

        ElementMatcher.Junction<NamedElement> whitelistMatcher = newListMatcher(predefWhitelist);
        if (runtimeWhitelist != null) whitelistMatcher = whitelistMatcher.or(newListMatcher(runtimeWhitelist.split(",")));

        ElementMatcher.Junction<NamedElement> blacklistMatcher = newListMatcher(predefBlacklist);
        if (runtimeBlacklist != null) blacklistMatcher = blacklistMatcher.or(newListMatcher(runtimeBlacklist.split(",")));

        ignoredMatcher = not(whitelistMatcher).or(blacklistMatcher);
    }

    private static ElementMatcher.Junction<NamedElement> newListMatcher(String[] packages) {
        ElementMatcher.Junction<NamedElement> matcher = none();
        for (String pkg : packages) {
            matcher = matcher.or(nameStartsWith(pkg));
        }
        return matcher;
    }

    static {
        String filterClass = System.getProperty("bytebuddy.debug.instrumentation.for.class");
        BYTE_BUDDY_LOGGING_FILTER = filterClass != null ? s -> s.contains(filterClass) : s -> false;
    }

    public void installOn(Instrumentation instrumentation) {
        this.agentBuilder.installOn(instrumentation);
    }

    public ResettableClassFileTransformer installOnByteBuddyAgent() {
        return this.agentBuilder.installOnByteBuddyAgent();
    }

}
