package com.criteo.hadoop.garmadon.agent.tracers.hadoop.nodemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.agent.tracers.Tracer;
import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.agent.utils.ReflectionHelper;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class ContainerResourceMonitoringTracerTest {


    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private ClassLoader classLoader;

    @Before
    public void setUp() throws IOException, ClassNotFoundException {
        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        Tracer.class,
                        MethodTracer.class,
                        ContainerResourceMonitoringTracer.class,
                        ContainerResourceMonitoringTracer.VcoreUsageTracer.class,
                        Class.forName(ContainerResourceMonitoringTracer.VcoreUsageTracer.class.getName() + "$SingletonHolder"),
                        ContainersMonitorImpl.class,
                        Class.forName(ContainersMonitorImpl.class.getName() + "$MonitoringThread"),
                        ContainerMetrics.class,
                        Class.forName(ContainerMetrics.class.getName() + "$1")
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void ContainerResourceMonitoringModule_should_attach_to_recordCpuUsage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        final Header[] header = new Header[1];
        final Object[] event = new Object[1];
        TriConsumer<Long, Header, Object> cons = (t, h, o) -> {
            header[0] = h;
            event[0] = o;
        };
        ReflectionHelper.setField(null, classLoader.loadClass(ContainerResourceMonitoringTracer.class.getName()), "eventHandler", cons);

        ClassFileTransformer classFileTransformer = new ContainerResourceMonitoringTracer.VcoreUsageTracer().installOnByteBuddyAgent();

        try {
            Class<?> clazz = classLoader.loadClass(ContainerMetrics.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(MetricsSystem.class, ContainerId.class, long.class, long.class);
            constructor.setAccessible(true);
            Object inFormat = constructor.newInstance(mock(MetricsSystem.class), ContainerId.fromString("container_e16940_1541383784275_24407_01_000023"), 5L, 5L);

            Method m = clazz.getDeclaredMethod("recordCpuUsage", int.class, int.class);
            m.invoke(inFormat, 10, 10000);

            assertNotNull(header[0]);
            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void ContainerResourceMonitoringModule_should_attach_to_isProcessTreeOverLimit() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        final Header[] header = new Header[1];
        final Object[] event = new Object[1];
        ContainerResourceMonitoringTracer.initEventHandler((t, h, o) -> {
            header[0] = h;
            event[0] = o;
        });
        ClassFileTransformer classFileTransformer = new ContainerResourceMonitoringTracer.MemorySizeTracer().installOnByteBuddyAgent();

        try {
            ContainerExecutor exec = mock(ContainerExecutor.class);
            AsyncDispatcher dispatcher = mock(AsyncDispatcher.class);
            Context ctx = mock(Context.class);

            Class<?> clazz = classLoader.loadClass(ContainersMonitorImpl.class.getName());
            Method m = clazz.getDeclaredMethod("isProcessTreeOverLimit", String.class, long.class, long.class, long.class);
            Object inFormat = clazz.getConstructor(ContainerExecutor.class, AsyncDispatcher.class, Context.class).newInstance(exec, dispatcher, ctx);

            m.setAccessible(true);
            m.invoke(inFormat, "container_e600_1516870220739_0069_01_000032", 2000, 1000, 3000);

            assertNotNull(header[0]);
            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

}
