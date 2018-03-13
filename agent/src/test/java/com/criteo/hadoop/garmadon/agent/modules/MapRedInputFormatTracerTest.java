package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.schema.events.PathEvent;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.mapred.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

/**
 * IMPORTANT NOTE, we use a specific classloader to allow class redefinition between tests
 * We need to use reflection even for method calls because class casting cannot be cross classloader
 * and the test itself is not loaded with our custom classloader
 */
public class MapRedInputFormatTracerTest {

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private Consumer<Object> eventHandler;
    private InputSplit inputSplit;
    private JobConf jobConf;
    private Reporter reporter;

    private ClassLoader classLoader;

    private String inputPath = "/some/inputpath,/some/other/path";
    private String deprecatedInputPath = "/some/inputpathDeprecated";

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(Consumer.class);
        inputSplit = mock(InputSplit.class);
        jobConf = mock(JobConf.class);
        reporter = mock(Reporter.class);

        when(jobConf.getJobName())
                .thenReturn("Application");
        when(jobConf.getUser())
                .thenReturn("user");

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        OneLevelHierarchy.class,
                        AbstractInputFormat.class,
                        RealInputFormat.class,
                        Level1.class,
                        Level2CallingSuper.class,
                        Level3NotCallingSuper.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(inputSplit);
        reset(jobConf);
        reset(reporter);
    }

    /*
        We want to test if we intercept method impl in a one level class hierarchy
     */
    @Test
    @AgentAttachmentRule.Enforce
    public void InputFormatTracer_should_intercept_InputFormat_direct_implementor() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir"))
                    .thenReturn(inputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(OneLevelHierarchy.class.getName());
            invokeRecordReader(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), inputPath, PathEvent.Type.INPUT);
            verify(eventHandler).accept(pathEvent);
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
        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapred.input.dir"))
                    .thenReturn(deprecatedInputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(OneLevelHierarchy.class.getName());
            invokeRecordReader(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), deprecatedInputPath, PathEvent.Type.INPUT);
            verify(eventHandler).accept(pathEvent);
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
        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir"))
                    .thenReturn("/some/path");

            //Call InputFormat
            Class<?> type = classLoader.loadClass(RealInputFormat.class.getName());
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
        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir"))
                    .thenReturn(inputPath);

            //Call InputFormat
            Class<?> type = classLoader.loadClass(Level2CallingSuper.class.getName());
            Object recordReader = invokeRecordReader(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), inputPath, PathEvent.Type.INPUT);
            verify(eventHandler, times(2)).accept(pathEvent);
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
        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //JobConf is prepared by Level3NotCallingSuper class
            //Testing PathEvent value shows Level3NotCallingSuper
            //was intercepted

            when(jobConf.get("mapreduce.input.fileinputformat.inputdir"))
                    .thenReturn("/not_calling_super");

            //Call InputFormat
            Class<?> type = classLoader.loadClass(Level3NotCallingSuper.class.getName());
            Object recordReader = invokeRecordReader(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), "/not_calling_super", PathEvent.Type.INPUT);
            verify(eventHandler, times(1)).accept(pathEvent);
            assertThat("", (boolean) type.getDeclaredField("isAccessed").get(null));
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    public void InputFormatTracer_should_let_getRecordReader_return_its_original_value() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, NoSuchFieldException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new MapReduceModule.DeprecatedInputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Prepare JobConf
            when(jobConf.get("mapreduce.input.fileinputformat.inputdir"))
                    .thenReturn("/some/path");

            Class<?> type = classLoader.loadClass(OneLevelHierarchy.class.getName());
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

    public static class OneLevelHierarchy implements InputFormat {

        public static RecordReader recordReaderMock = mock(RecordReader.class);

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return recordReaderMock;
        }
    }

    public static abstract class AbstractInputFormat implements InputFormat {

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        abstract public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter);
    }

    public static class RealInputFormat extends AbstractInputFormat {

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) {
            return null;
        }
    }

    public static class Level1 implements InputFormat {

        static RecordReader recordReaderMock = mock(RecordReader.class);

        @Override
        public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
            throw new RuntimeException("not supposed to be used");
        }

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            return recordReaderMock;
        }
    }

    public static class Level2CallingSuper extends Level1 {

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            System.out.println("Do something before");
            RecordReader rr = super.getRecordReader(inputSplit, jobConf, reporter);
            System.out.println("Do something after");
            return rr;
        }
    }

    public static class Level3NotCallingSuper extends Level2CallingSuper {

        static RecordReader recordReaderMock = mock(RecordReader.class);
        public static boolean isAccessed = false;

        @Override
        public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
            isAccessed = true;
            return recordReaderMock;
        }
    }

}
