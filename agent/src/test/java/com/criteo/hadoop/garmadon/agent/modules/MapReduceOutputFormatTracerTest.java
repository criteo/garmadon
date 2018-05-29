package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.schema.events.PathEvent;
import com.criteo.hadoop.garmadonnotexcluded.MapReduceOutputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.After;
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
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class MapReduceOutputFormatTracerTest {
    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private Consumer<Object> eventHandler;
    private TaskAttemptContext taskAttemptContext;
    private Configuration conf;

    private ClassLoader classLoader;

    private String outputPath = "/some/outputpath,/some/other/path";
    private String deprecatedOutputPath = "/some/outputpathDeprecated";

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(Consumer.class);
        taskAttemptContext = mock(TaskAttemptContext.class);
        conf = new Configuration();
        conf.set("mapreduce.output.fileoutputformat.outputdir", outputPath);

        when(taskAttemptContext.getConfiguration())
                .thenReturn(conf);
        when(taskAttemptContext.getJobName())
                .thenReturn("Application");
        when(taskAttemptContext.getUser())
                .thenReturn("user");

        JobID jobId = mock(JobID.class);
        when(jobId.toString())
                .thenReturn("app_1");
        when(taskAttemptContext.getJobID())
                .thenReturn(jobId);

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        MapReduceOutputFormatTestClasses.OneLevelHierarchy.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(taskAttemptContext);
    }

    /*
        We want to test if we intercept method impl in a one level class hierarchy
     */
    @Test
    @AgentAttachmentRule.Enforce
    public void OutputFormatTracer_should_intercept_InputFormat_direct_implementor() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceModule.OutputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapReduceOutputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordWriter(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), outputPath, PathEvent.Type.OUTPUT);
            verify(eventHandler, times(1)).accept(pathEvent);
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    /*
        We want to test that we get deprecated path if mapreduce one is not provided
     */
    @Test
    public void OutputFormatTracer_should_should_get_deprecated_value() throws Exception {
        // Configure deprecated output dir
        conf.unset("mapreduce.output.fileoutputformat.outputdir");
        conf.set("mapred.output.dir", deprecatedOutputPath);

        Constructor<MapReduceModule.OutputFormatTracer> c = MapReduceModule.OutputFormatTracer.class.getDeclaredConstructor(Consumer.class);
        c.setAccessible(true);
        MapReduceModule.OutputFormatTracer u = c.newInstance(eventHandler);
        u.intercept(taskAttemptContext);
        PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), deprecatedOutputPath, PathEvent.Type.OUTPUT);
        verify(eventHandler).accept(pathEvent);
    }

    private Object invokeRecordWriter(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getRecordWriter", TaskAttemptContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, taskAttemptContext);
    }

}
