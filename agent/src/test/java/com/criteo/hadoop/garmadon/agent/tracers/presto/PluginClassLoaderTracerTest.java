package com.criteo.hadoop.garmadon.agent.tracers.presto;

import com.criteo.hadoop.garmadon.agent.tracers.ConstructorTracer;
import com.criteo.hadoop.garmadon.agent.tracers.Tracer;
import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.google.common.collect.ImmutableList;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class PluginClassLoaderTracerTest {
    private static ClassLoader classLoader;

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @BeforeClass
    public static void setUpClass() throws IOException, ClassNotFoundException {
        classLoader = new ByteArrayClassLoader.ChildFirst(PluginClassLoaderTracerTest.class.getClassLoader(),
                ClassFileExtraction.of(
                        Tracer.class,
                        ConstructorTracer.class,
                        PluginClassLoaderTracer.class,
                        Class.forName("com.facebook.presto.server.PluginClassLoader")
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void RMAppTracer_should_not_failed_attaching_RMContextImpl_constructior_due_to_TriConsumer_visibility()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException, MalformedURLException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));


        ClassFileTransformer classFileTransformer = new PluginClassLoaderTracer.GarmadonLoadScope().installOnByteBuddyAgent();

        try {
            Class<?> clazz = classLoader.loadClass("com.facebook.presto.server.PluginClassLoader");

            Constructor<?> constructor = clazz.getDeclaredConstructor(List.class, ClassLoader.class, Iterable.class);
            constructor.setAccessible(true);

            List<URL> urls = new ArrayList();
            urls.add(new URL("file:///test"));

            ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
                    .add("com.facebook.presto.spi.")
                    .add("com.fasterxml.jackson.annotation.")
                    .add("io.airlift.slice.")
                    .add("io.airlift.units.")
                    .add("org.openjdk.jol.")
                    .build();

            Object pluginClassLoader = constructor.newInstance(urls,
                    classLoader,
                    SPI_PACKAGES);

            Field field = clazz.getDeclaredField("spiPackages");
            field.setAccessible(true);
            List<String> spiPackages = (List) field.get(pluginClassLoader);

            assertEquals(ImmutableList.<String>builder()
                    .add("com.facebook.presto.spi.")
                    .add("com.fasterxml.jackson.annotation.")
                    .add("io.airlift.slice.")
                    .add("io.airlift.units.")
                    .add("org.openjdk.jol.")
                    .add("com.criteo.hadoop.garmadon.agent.")
                    .build(), spiPackages);


        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

}
