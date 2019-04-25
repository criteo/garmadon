package com.criteo.hadoop.garmadon.agent.tracers.presto;

import com.criteo.hadoop.garmadon.agent.tracers.ConstructorTracer;
import com.google.common.collect.ImmutableList;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;


public class PluginClassLoaderTracer {

    public static final String PRESTOSQL_PLUGINCLASSLOADER = "io.prestosql.server.PluginClassLoader";
    public static final String PRESTODB_PLUGINCLASSLOADER = "com.facebook.presto.server.PluginClassLoader";

    protected PluginClassLoaderTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Instrumentation instrumentation) {
        new PluginClassLoaderTracer.GarmadonLoadScope().installOn(instrumentation);
    }

    public static class GarmadonLoadScope extends ConstructorTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return named(PRESTOSQL_PLUGINCLASSLOADER).or(named(PRESTODB_PLUGINCLASSLOADER));
        }

        @Override
        protected ElementMatcher<? super MethodDescription> constructorMatcher() {
            return takesArguments(4);
        }

        @Override
        protected Implementation newImplementation() {
            return Advice.to(PluginClassLoaderTracer.GarmadonLoadScope.class);
        }

        @Advice.OnMethodExit
        public static void addGarmadonAgentPackage(@Advice.This Object o) throws Exception {
            Class pluginClassLoaderZz = o.getClass().getClassLoader().loadClass(o.getClass().getName());
            Field spiClassLoaderField = pluginClassLoaderZz.getDeclaredField("spiPackages");
            spiClassLoaderField.setAccessible(true);

            ImmutableList<String> spiPackages = (ImmutableList<String>) spiClassLoaderField.get(o);
            List<String> tmpspiPackages = new ArrayList<>(spiPackages);
            tmpspiPackages.add("com.criteo.hadoop.garmadon.agent.");

            spiClassLoaderField.set(o, ImmutableList.copyOf(tmpspiPackages));
        }
    }
}
