package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.agent.tracers.Tracer;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer;
import com.criteo.hadoop.garmadon.agent.tracers.hadoop.nodemanager.ContainerResourceMonitoringTracer;
import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.agent.utils.ReflectionHelper;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.*;
import org.junit.*;
import org.junit.rules.MethodRule;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class RMappTracerTest {
    private static ClassLoader classLoader;


    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @BeforeClass
    public static void setUpClass() throws IOException {
        classLoader = new ByteArrayClassLoader.ChildFirst(RMappTracerTest.class.getClassLoader(),
                ClassFileExtraction.of(
                        Tracer.class,
                        MethodTracer.class,
                        RMAppTracer.class,
                        RMAppTracer.RMContextImplThread.class,
                        RMContextImplEventRunnable.class,
                        RMContextImpl.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void RMAppTracer_should_not_failed_attaching_RMContextImpl_constructior_due_to_TriConsumer_visibility() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException, IOException, InstantiationException, NoSuchFieldException, InterruptedException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        final Header[] header = new Header[1];
        final Object[] event = new Object[1];
        TriConsumer<Long, Header, Object> cons = (t, h, o) -> {
            header[0] = h;
            event[0] = o;
        };
        ReflectionHelper.setField(null, classLoader.loadClass(RMAppTracer.class.getName()), "eventHandler", cons);

        ClassFileTransformer classFileTransformer = new RMAppTracer.RMContextImplThread().installOnByteBuddyAgent();

        try {
            Class<?> clazz = classLoader.loadClass(RMContextImpl.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Dispatcher.class, ContainerAllocationExpirer.class,
                    AMLivelinessMonitor.class, AMLivelinessMonitor.class, DelegationTokenRenewer.class, AMRMTokenSecretManager.class,
                    RMContainerTokenSecretManager.class, NMTokenSecretManagerInRM.class, ClientToAMTokenSecretManagerInRM.class,
                    RMApplicationHistoryWriter.class);

            constructor.newInstance(mock(Dispatcher.class),
                    mock(ContainerAllocationExpirer.class),
                    mock(AMLivelinessMonitor.class),
                    mock(AMLivelinessMonitor.class),
                    mock(DelegationTokenRenewer.class),
                    mock(AMRMTokenSecretManager.class),
                    mock(RMContainerTokenSecretManager.class),
                    mock(NMTokenSecretManagerInRM.class),
                    mock(ClientToAMTokenSecretManagerInRM.class),
                    mock(RMApplicationHistoryWriter.class));
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

}
