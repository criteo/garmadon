package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.schema.events.PathEvent;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.lang.instrument.Instrumentation;
import java.util.function.Consumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.isSubTypeOf;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class MapReduceModule extends ContainerModule {

    // Output format configuration
    private static final String DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR = "mapred.output.dir";
    private static final String FILE_OUTPUT_FORMAT_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";

    // Input format configuration
    private static final String DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR = "mapred.input.dir";
    private static final String FILE_INPUT_FORMAT_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";

    @Override
    public void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer) {
        new InputFormatTracer(eventConsumer::accept).installOn(instrumentation);
        new OutputFormatTracer(eventConsumer::accept).installOn(instrumentation);
        new DeprecatedInputFormatTracer(eventConsumer::accept).installOn(instrumentation);
        new DeprecatedOutputFormatTracer(eventConsumer::accept).installOn(instrumentation);
    }

    public static class InputFormatTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public InputFormatTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(org.apache.hadoop.mapreduce.InputFormat.class);
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getSplits").or(named("createRecordReader"));
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@Argument(0) JobContext jobContext) {
            String paths = (jobContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    jobContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) : jobContext.getConfiguration().get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), paths, PathEvent.Type.INPUT);
                eventHandler.accept(pathEvent);
            }
        }

        public void intercept(@Argument(1) TaskAttemptContext taskAttemptContext) {
            String paths = (taskAttemptContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    taskAttemptContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) : taskAttemptContext.getConfiguration().get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), paths, PathEvent.Type.INPUT);
                eventHandler.accept(pathEvent);
            }
        }
    }

    public static class OutputFormatTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public OutputFormatTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(org.apache.hadoop.mapreduce.OutputFormat.class);
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordWriter");
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@Argument(0) TaskAttemptContext taskAttemptContext) {
            String paths = (taskAttemptContext.getConfiguration().get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) != null) ?
                    taskAttemptContext.getConfiguration().get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) : taskAttemptContext.getConfiguration().get(DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR);
            if (paths != null) {
                PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), paths, PathEvent.Type.OUTPUT);
                eventHandler.accept(pathEvent);
            }
        }
    }

    public static class DeprecatedInputFormatTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public DeprecatedInputFormatTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(org.apache.hadoop.mapred.InputFormat.class);
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordReader");
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@Argument(1) JobConf jobConf) throws Exception {
            String paths = (jobConf.get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    jobConf.get(FILE_INPUT_FORMAT_INPUT_DIR) : jobConf.get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), paths, PathEvent.Type.INPUT);
                eventHandler.accept(pathEvent);
            }
        }
    }

    public static class DeprecatedOutputFormatTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public DeprecatedOutputFormatTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(org.apache.hadoop.mapred.OutputFormat.class);
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordWriter");
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@Argument(1) JobConf jobConf) {
            String paths = (jobConf.get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) != null) ?
                    jobConf.get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) : jobConf.get(DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR);
            if (paths != null) {
                PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), paths, PathEvent.Type.OUTPUT);
                eventHandler.accept(pathEvent);
            }
        }
    }

}
