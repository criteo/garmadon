package com.criteo.hadoop.garmadon.agent.tracers.hadoop.mapreduce;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.PathType;
import com.criteo.hadoop.garmadonnotexcluded.MapRedOutputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
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
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MapRedOutputFormatTracerTest {
    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private BiConsumer<Long, Object> eventHandler;
    private JobConf jobConf;

    private ClassLoader classLoader;

    private String outputPath = "/some/outputpath,/some/other/path";
    private String deprecatedOutputPath = "/some/outputpathDeprecated";

    private ArgumentCaptor<DataAccessEventProtos.PathEvent> argument;

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(BiConsumer.class);
        argument = ArgumentCaptor.forClass(DataAccessEventProtos.PathEvent.class);

        MapReduceTracer.initEventHandler(eventHandler);
        jobConf = mock(JobConf.class);

        when(jobConf.getJobName())
                .thenReturn("Application");
        when(jobConf.getUser())
                .thenReturn("user");

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        MapRedOutputFormatTestClasses.OneLevelHierarchy.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(jobConf);
    }

    @Test
    public void OutputFormatTracer_should_use_a_latent_type_definition_equivalent_to_the_ForLoadedType_one(){
        TypeDescription realTypeDef = TypeDescription.ForLoadedType.of(org.apache.hadoop.mapred.OutputFormat.class);
        TypeDescription latentTypeDef = MapReduceTracer.Types.MAPRED_OUTPUT_FORMAT.getTypeDescription();

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

        // Prepare jobConf
        when(jobConf.get("mapreduce.output.fileoutputformat.outputdir"))
                .thenReturn(outputPath);

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedOutputFormatTracer().installOnByteBuddyAgent();
        try {
            //Call OnputFormat
            Class<?> type = classLoader.loadClass(MapRedOutputFormatTestClasses.OneLevelHierarchy.class.getName());
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
        when(jobConf.get("mapred.output.dir"))
                .thenReturn(deprecatedOutputPath);

        MapReduceTracer.DeprecatedOutputFormatTracer.intercept(jobConf);

        verify(eventHandler).accept(any(Long.class), argument.capture());
        DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setPath(deprecatedOutputPath)
                .setType(PathType.OUTPUT.name())
                .build();
        assertEquals(pathEvent, argument.getValue());
    }

    private Object invokeRecordWriter(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getRecordWriter", FileSystem.class, JobConf.class, String.class, Progressable.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, mock(FileSystem.class), jobConf, "test", mock(Progressable.class));
    }

}
