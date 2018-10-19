package com.criteo.hadoop.garmadon.agent.tracers.hadoop.mapreduce;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.PathType;
import com.criteo.hadoop.garmadonnotexcluded.MapReduceOutputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
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

public class MapReduceOutputFormatTracerTest {
    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private BiConsumer<Long, Object> eventHandler;
    private TaskAttemptContext taskAttemptContext;
    private Configuration conf;

    private ClassLoader classLoader;

    private String outputPath = "/some/outputpath,/some/other/path";
    private String deprecatedOutputPath = "/some/outputpathDeprecated";

    private ArgumentCaptor<DataAccessEventProtos.PathEvent> argument;

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(BiConsumer.class);
        argument = ArgumentCaptor.forClass(DataAccessEventProtos.PathEvent.class);

        MapReduceTracer.initEventHandler(eventHandler);
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

    @Test
    public void OutputFormatTracer_should_use_a_latent_type_definition_equivalent_to_the_ForLoadedType_one(){
        TypeDescription realTypeDef = TypeDescription.ForLoadedType.of(org.apache.hadoop.mapreduce.OutputFormat.class);
        TypeDescription latentTypeDef = MapReduceTracer.Types.MAPREDUCE_OUTPUT_FORMAT.getTypeDescription();

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
    public void OutputFormatTracer_should_intercept_InputFormat_direct_implementor() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.OutputFormatTracer().installOnByteBuddyAgent();
        try {
            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapReduceOutputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordWriter(type);

            //Verify mock interaction
            verify(eventHandler).accept(any(Long.class), argument.capture());
            DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                    .newBuilder()
                    .setPath(outputPath)
                    .setType(PathType.OUTPUT.name())
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
    public void OutputFormatTracer_should_get_deprecated_value() throws Exception {
        // Configure deprecated output dir
        conf.unset("mapreduce.output.fileoutputformat.outputdir");
        conf.set("mapred.output.dir", deprecatedOutputPath);

        MapReduceTracer.OutputFormatTracer.intercept(taskAttemptContext);
        verify(eventHandler).accept(any(Long.class), argument.capture());
        DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setPath(deprecatedOutputPath)
                .setType(PathType.OUTPUT.name())
                .build();
        assertEquals(pathEvent, argument.getValue());
    }

    private Object invokeRecordWriter(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getRecordWriter", TaskAttemptContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, taskAttemptContext);
    }

}
