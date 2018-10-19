package com.criteo.hadoop.garmadon.agent.tracers.hadoop.mapreduce;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.PathType;
import com.criteo.hadoop.garmadonnotexcluded.MapRedInputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
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
public class MapRedInputFormatTracerTest {

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private BiConsumer<Long, Object> eventHandler;
    private InputSplit inputSplit;
    private JobConf jobConf;
    private Reporter reporter;

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
        jobConf = mock(JobConf.class);
        reporter = mock(Reporter.class);

        when(jobConf.getJobName()).thenReturn("Application");
        when(jobConf.getUser()).thenReturn("user");

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        MapRedInputFormatTestClasses.OneLevelHierarchy.class,
                        MapRedInputFormatTestClasses.AbstractInputFormat.class,
                        MapRedInputFormatTestClasses.RealInputFormat.class,
                        MapRedInputFormatTestClasses.Level1.class,
                        MapRedInputFormatTestClasses.Level2CallingSuper.class,
                        MapRedInputFormatTestClasses.Level3NotCallingSuper.class),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(inputSplit);
        reset(jobConf);
        reset(reporter);
    }

    @Test
    public void InputFormatTracer_should_not_fail_when_instrumentation_happens_in_a_separate_classloader() throws ClassNotFoundException, IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();

        try {
            ClassLoader appCl = getClass().getClassLoader();
            ClassLoader cl = new ClassLoader() {
                @Override
                public Class<?> loadClass(String name) throws ClassNotFoundException {
                    if (!name.startsWith("java")) {
                        InputStream is = appCl.getResourceAsStream(name.replace(".", "/") + ".class");
                        ByteArrayOutputStream byteSt = new ByteArrayOutputStream();
                        //write into byte
                        int len = 0;
                        try {
                            while ((len = is.read()) != -1) {
                                byteSt.write(len);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        //convert into byte array
                        byte[] bt = byteSt.toByteArray();
                        return defineClass(name, bt, 0, bt.length);
                    } else return appCl.loadClass(name);
                }

                ;

                @Override
                public InputStream getResourceAsStream(String name) {
                    return appCl.getResourceAsStream(name);
                }
            };

            Class<?> type = cl.loadClass(MapRedInputFormatTestClasses.SimpleInputFormat.class.getName());
            type.newInstance();
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }

    }

    @Test
    public void InputFormatTracer_should_use_a_latent_type_definition_equivalent_to_the_ForLoadedType_one() {
        TypeDescription realTypeDef = TypeDescription.ForLoadedType.of(org.apache.hadoop.mapred.InputFormat.class);
        TypeDescription latentTypeDef = MapReduceTracer.Types.MAPRED_INPUT_FORMAT.getTypeDescription();

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
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir")).thenReturn(inputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordReader(type);

            //Verify mock interaction
            verify(eventHandler).accept(any(Long.class), argument.capture());
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
        We want to test if we intercept method impl in a one level class hierarchy for deprecated input dir
     */
    @Test
    @AgentAttachmentRule.Enforce
    public void InputFormatTracer_should_intercept_InputFormat_direct_implementor_for_deprecated_inputdir() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapred.input.dir")).thenReturn(deprecatedInputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordReader(type);

            //Verify mock interaction
            verify(eventHandler).accept(any(Long.class), argument.capture());
            DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                    .newBuilder()
                    .setPath(deprecatedInputPath)
                    .setType(PathType.INPUT.name())
                    .build();
            assertEquals(pathEvent, argument.getValue());
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    /*
        We want to test if we dont try (and so fail) to intercept
        abstract methods
     */
    @Test
    public void InputFormatTracer_should_not_intercept_abstract_method() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir")).thenReturn("/some/path");

            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.RealInputFormat.class.getName());
            invokeRecordReader(type);

            //We just want to test no exception because of presence of abstract method and abstract class
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    /*
        We should implement first method implementation in class hierarchy
     */
    @Test
    public void InputFormatTracer_should_intercept_first_method_impl_in_hierarchy() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir")).thenReturn(inputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.Level2CallingSuper.class.getName());
            Object recordReader = invokeRecordReader(type);

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
        We should only intercept a child class impl of it does not call super
        If not, we would trace to many events
     */
    @Test
    public void InputFormatTracer_should_intercept_child_class_if_not_calling_super() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //JobConf is prepared by Level3NotCallingSuper class
            //Testing PathEvent value shows Level3NotCallingSuper
            //was intercepted

            when(jobConf.get("mapreduce.input.fileinputformat.inputdir")).thenReturn("/not_calling_super");

            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.Level3NotCallingSuper.class.getName());
            Object recordReader = invokeRecordReader(type);

            //Verify mock interaction
            verify(eventHandler).accept(any(Long.class), argument.capture());
            DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                    .newBuilder()
                    .setPath("/not_calling_super")
                    .setType(PathType.INPUT.name())
                    .build();
            assertEquals(pathEvent, argument.getValue());
            assertThat("", (boolean) type.getDeclaredField("isAccessed").get(null));
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    public void InputFormatTracer_should_let_getRecordReader_return_its_original_value() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new MapReduceTracer.DeprecatedInputFormatTracer().installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir")).thenReturn("/some/path");

            Class<?> type = classLoader.loadClass(MapRedInputFormatTestClasses.OneLevelHierarchy.class.getName());
            Object recordReader = invokeRecordReader(type);

            assertThat(recordReader, equalTo(getRecordReaderMockReflection(type)));
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    private Object invokeRecordReader(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getRecordReader", InputSplit.class, JobConf.class, Reporter.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, inputSplit, jobConf, reporter);
    }

    private Object getRecordReaderMockReflection(Class<?> type) throws NoSuchFieldException, IllegalAccessException {
        return type.getDeclaredField("recordReaderMock").get(null);
    }

}
