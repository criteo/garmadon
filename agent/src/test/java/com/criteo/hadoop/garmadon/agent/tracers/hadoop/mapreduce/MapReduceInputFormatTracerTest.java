package com.criteo.hadoop.garmadon.agent.tracers.hadoop.mapreduce;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.PathType;
import com.criteo.hadoop.garmadonnotexcluded.MapReduceInputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * IMPORTANT NOTE, we use a specific classloader to allow class redefinition between tests
 * We need to use reflection even for method calls because class casting cannot be cross classloader
 * and the test itself is not loaded with our custom classloader
 */
public class MapReduceInputFormatTracerTest {

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private BiConsumer<Long, Object> eventHandler;
    private InputSplit inputSplit;
    private JobContext jobContext;
    private TaskAttemptContext taskAttemptContext;
    private Configuration conf;

    private ClassLoader classLoader;

    private String inputPath = "/some/inputpath,/some/other/path";
    private String deprecatedInputPath = "/some/inputpathDeprecated";

    private ArgumentCaptor<DataAccessEventProtos.PathEvent> argument;

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(BiConsumer.class);
        argument = ArgumentCaptor.forClass(DataAccessEventProtos.PathEvent.class);

        MapReduceTracer.initEventHandler(eventHandler);
        inputSplit = mock(InputSplit.class);
        jobContext = mock(JobContext.class);
        taskAttemptContext = mock(TaskAttemptContext.class);
        conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.inputdir", inputPath);

        when(jobContext.getConfiguration())
                .thenReturn(conf);
        when(taskAttemptContext.getConfiguration())
                .thenReturn(conf);

        when(jobContext.getJobName())
                .thenReturn("Application");
        when(jobContext.getUser())
                .thenReturn("user");
        when(taskAttemptContext.getJobName())
                .thenReturn("Application");
        when(taskAttemptContext.getUser())
                .thenReturn("user");

        JobID jobId = mock(JobID.class);
        when(jobId.toString())
                .thenReturn("app_1");
        when(jobContext.getJobID())
                .thenReturn(jobId);
        when(taskAttemptContext.getJobID())
                .thenReturn(jobId);

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        MapReduceInputFormatTestClasses.OneLevelHierarchy.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(inputSplit);
        reset(jobContext);
        reset(taskAttemptContext);
    }

    @Test
    public void InputFormatTracer_should_use_a_latent_type_definition_equivalent_to_the_ForLoadedType_one(){
        TypeDescription realTypeDef = TypeDescription.ForLoadedType.of(org.apache.hadoop.mapreduce.InputFormat.class);
        TypeDescription latentTypeDef = MapReduceTracer.Types.MAPREDUCE_INPUT_FORMAT.getTypeDescription();

        assertThat(latentTypeDef.getName(), is(realTypeDef.getName()));
        assertThat(latentTypeDef.getModifiers(), is(realTypeDef.getModifiers()));
        assertThat(latentTypeDef.getSuperClass(), is(realTypeDef.getSuperClass()));
        assertThat(latentTypeDef.getInterfaces(), is(realTypeDef.getInterfaces()));
    }

    /*
        We want to test if we intercept method impl in a one level class hierarchy
     */
    @Test
    @AgentAttachmentRule.Enforce
    public void InputFormatTracer_should_intercept_InputFormat_direct_implementor() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.InputFormatTracer().installOnByteBuddyAgent();
        try {
            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapReduceInputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordReader(type);
            invokeListInputSplits(type);

            //Verify mock interaction
            verify(eventHandler, times(2)).accept(any(Long.class), argument.capture());
            DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                    .newBuilder()
                    .setPath(inputPath)
                    .setType(PathType.INPUT.name())
                    .build();
            assertEquals(pathEvent, argument.getValue());
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    /*
        We want to test that we get deprecated path if mapreduce one is not provided
     */
    @Test
    public void InputFormatTracer_should_get_deprecated_value() throws Exception {
        // Configure deprecated output dir
        conf.unset("mapreduce.input.fileinputformat.inputdir");
        conf.set("mapred.input.dir", deprecatedInputPath);

        MapReduceTracer.InputFormatTracer.intercept(taskAttemptContext);
        verify(eventHandler).accept(any(Long.class), argument.capture());
        DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setPath(deprecatedInputPath)
                .setType(PathType.INPUT.name())
                .build();
        assertEquals(pathEvent, argument.getValue());
    }

    private Object invokeRecordReader(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("createRecordReader", InputSplit.class, TaskAttemptContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, inputSplit, taskAttemptContext);
    }

    private Object invokeListInputSplits(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getSplits", JobContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, jobContext);
    }


}
