package com.criteo.hadoop.garmadon.agent.tracers;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener.Filtering;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.NamedElement;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.*;

public abstract class Tracer {

    protected AgentBuilder agentBuilder;

    static ElementMatcher.Junction<NamedElement> ignoredMatcher;
    static {
        String[] predefWhitelist = {
                "org.apache.flink.",
                "org.apache.hadoop.",
                "org.apache.spark.",
                "com.criteo."
        };
        String[] predefBlacklist = {
                "com.criteo.garmadon.",
                "com.criteo.jvm."
        };
        String runtimeWhitelist = System.getProperty("bytebuddy.whitelist.for.instrumentation");
        String runtimeBlacklist = System.getProperty("bytebuddy.blacklist.for.instrumentation");

        ElementMatcher.Junction<NamedElement> whitelistMatcher = newListMatcher(predefWhitelist);
        if(runtimeWhitelist != null) whitelistMatcher = whitelistMatcher.or(newListMatcher(runtimeWhitelist.split(",")));

        ElementMatcher.Junction<NamedElement> blacklistMatcher =  newListMatcher(predefBlacklist);
        if(runtimeBlacklist != null) blacklistMatcher = blacklistMatcher.or(newListMatcher(runtimeBlacklist.split(",")));

        ignoredMatcher = not(whitelistMatcher).or(blacklistMatcher);
    }

    private static ElementMatcher.Junction<NamedElement> newListMatcher(String[] packages) {
        ElementMatcher.Junction<NamedElement> matcher = none();
        for (String pkg : packages) {
            matcher = matcher.or(nameStartsWith(pkg));
        }
        return matcher;
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
                .ignore(ignoredMatcher)
                .with(
                        new Filtering(byteBuddyLoggingFilter, AgentBuilder.Listener.StreamWriting.toSystemOut())
                );
    }

    public void installOn(Instrumentation instrumentation) {
        this.agentBuilder.installOn(instrumentation);
    }

    public ResettableClassFileTransformer installOnByteBuddyAgent() {
        return this.agentBuilder.installOnByteBuddyAgent();
    }

}
