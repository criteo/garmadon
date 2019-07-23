package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.agent.tracers.Tracer;
import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
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
import static org.mockito.Mockito.when;

public class RMContainerTracerTest {
    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private ClassLoader classLoader;

    @Before
    public void setUp() throws IOException {
        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
            ClassFileExtraction.of(
                Tracer.class,
                MethodTracer.class,
                RMContainerTracer.class,
                RMContainerTracer.RMContainerImplTracer.class
            ),
            ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void ContainerResourceMonitoringModule_should_attach_to_recordCpuUsage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        final Header[] header = new Header[1];
        final Object[] event = new Object[1];
        RMContainerTracer.initEventHandler((t, h, o) -> {
            header[0] = h;
            event[0] = o;
        });

        ClassFileTransformer classFileTransformer = new RMContainerTracer.RMContainerImplTracer().installOnByteBuddyAgent();

        try {
            Class<?> clazz = classLoader.loadClass(RMContainerImpl.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Container.class,
                ApplicationAttemptId.class, NodeId.class, String.class,
                RMContext.class);
            constructor.setAccessible(true);

            RMContext rmContext = mock(RMContext.class);
            Configuration yarnConf = new Configuration();
            when(rmContext.getYarnConfiguration())
                .thenReturn(yarnConf);
            Dispatcher dispatcher = mock(Dispatcher.class);
            EventHandler eventHandler = mock(EventHandler.class);
            when(dispatcher.getEventHandler())
                .thenReturn(eventHandler);
            when(rmContext.getDispatcher())
                .thenReturn(dispatcher);

            RMApplicationHistoryWriter rmApplicationHistoryWriter = mock(RMApplicationHistoryWriter.class);
            when(rmContext.getRMApplicationHistoryWriter())
                .thenReturn(rmApplicationHistoryWriter);

            SystemMetricsPublisher systemMetricsPublisher = mock(SystemMetricsPublisher.class);
            when(rmContext.getSystemMetricsPublisher())
                .thenReturn(systemMetricsPublisher);

            Object inFormat = constructor.newInstance(mock(Container.class), mock(ApplicationAttemptId.class), mock(NodeId.class), "user", rmContext);

            Method m = clazz.getDeclaredMethod("handle", RMContainerEvent.class);

            ContainerId cid = mock(ContainerId.class);
            ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);
            when(cid.toString())
                .thenReturn("cid");
            when(cid.getApplicationAttemptId())
                .thenReturn(applicationAttemptId);
            when(applicationAttemptId.toString())
                .thenReturn("appattempt_id");
            ApplicationId applicationId = mock(ApplicationId.class);
            when(applicationAttemptId.getApplicationId())
                .thenReturn(applicationId);
            when(applicationId.toString())
                .thenReturn("app_id");
            m.invoke(inFormat, new RMContainerEvent(cid, RMContainerEventType.START));

            assertNotNull(header[0]);
            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

}
