package com.criteo.hadoop.garmadon.agent.tracers.hadoop.mapreduce;

import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.PathType;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.lang.instrument.Instrumentation;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.isSubTypeOf;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class MapReduceTracer {

    private static BiConsumer<Long, Object> eventHandler;

    // Output format configuration
    private static final String DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR = "mapred.output.dir";
    private static final String FILE_OUTPUT_FORMAT_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";

    // Input format configuration
    private static final String DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR = "mapred.input.dir";
    private static final String FILE_INPUT_FORMAT_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";

    enum Types {

        MAPRED_INPUT_FORMAT("org.apache.hadoop.mapred.InputFormat", Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT),
        MAPRED_OUTPUT_FORMAT("org.apache.hadoop.mapred.OutputFormat", Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT),
        MAPREDUCE_INPUT_FORMAT("org.apache.hadoop.mapreduce.InputFormat", Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT, TypeDescription.Generic.OBJECT),
        MAPREDUCE_OUTPUT_FORMAT("org.apache.hadoop.mapreduce.OutputFormat", Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT, TypeDescription.Generic.OBJECT);

        private final TypeDescription typeDescription;

        Types(String name, int modifiers) {
            this(name, modifiers, null);
        }

        Types(String name, int modifiers, TypeDescription.Generic superClass) {
            this.typeDescription = new TypeDescription.Latent(name, modifiers, superClass);
        }

        public TypeDescription getTypeDescription() {
            return typeDescription;
        }
    }

    public static void setup(Instrumentation instrumentation, BiConsumer<Long, Object> eventConsumer) {

        initEventHandler(eventConsumer);

        new InputFormatTracer().installOn(instrumentation);
        new OutputFormatTracer().installOn(instrumentation);
        new DeprecatedInputFormatTracer().installOn(instrumentation);
        new DeprecatedOutputFormatTracer().installOn(instrumentation);
    }

    public static void initEventHandler(BiConsumer<Long, Object> eventConsumer) {
        MapReduceTracer.eventHandler = eventConsumer;
    }

    public static class InputFormatTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(Types.MAPREDUCE_INPUT_FORMAT.getTypeDescription());
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getSplits").or(named("createRecordReader"));
        }

        @Override
        protected Implementation newImplementation() {
            return to(InputFormatTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(0) JobContext jobContext) {
            String paths = (jobContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    jobContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) : jobContext.getConfiguration().get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                        .newBuilder()
                        .setPath(paths)
                        .setType(PathType.INPUT.name())
                        .build();
                eventHandler.accept(System.currentTimeMillis(), pathEvent);
            }
        }

        public static void intercept(@Argument(1) TaskAttemptContext taskAttemptContext) {
            String paths = (taskAttemptContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    taskAttemptContext.getConfiguration().get(FILE_INPUT_FORMAT_INPUT_DIR) : taskAttemptContext.getConfiguration().get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                        .newBuilder()
                        .setPath(paths)
                        .setType(PathType.INPUT.name())
                        .build();
                eventHandler.accept(System.currentTimeMillis(), pathEvent);
            }
        }
    }

    public static class OutputFormatTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(Types.MAPREDUCE_OUTPUT_FORMAT.getTypeDescription());
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordWriter");
        }

        @Override
        protected Implementation newImplementation() {
            return to(OutputFormatTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(0) TaskAttemptContext taskAttemptContext) {
            String paths = (taskAttemptContext.getConfiguration().get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) != null) ?
                    taskAttemptContext.getConfiguration().get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) : taskAttemptContext.getConfiguration().get(DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR);
            if (paths != null) {
                DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                        .newBuilder()
                        .setPath(paths)
                        .setType(PathType.OUTPUT.name())
                        .build();
                eventHandler.accept(System.currentTimeMillis(), pathEvent);
            }
        }
    }

    public static class DeprecatedInputFormatTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(Types.MAPRED_INPUT_FORMAT.getTypeDescription());
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordReader");
        }

        @Override
        protected Implementation newImplementation() {
            return to(DeprecatedInputFormatTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(1) JobConf jobConf) throws Exception {
            String paths = (jobConf.get(FILE_INPUT_FORMAT_INPUT_DIR) != null) ?
                    jobConf.get(FILE_INPUT_FORMAT_INPUT_DIR) : jobConf.get(DEPRECATED_FILE_INPUT_FORMAT_INPUT_DIR);
            if (paths != null) {
                DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                        .newBuilder()
                        .setPath(paths)
                        .setType(PathType.INPUT.name())
                        .build();
                eventHandler.accept(System.currentTimeMillis(), pathEvent);
            }
        }
    }

    public static class DeprecatedOutputFormatTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return isSubTypeOf(Types.MAPRED_OUTPUT_FORMAT.getTypeDescription());
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getRecordWriter");
        }

        @Override
        protected Implementation newImplementation() {
            return to(DeprecatedOutputFormatTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(1) JobConf jobConf) {
            String paths = (jobConf.get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) != null) ?
                    jobConf.get(FILE_OUTPUT_FORMAT_OUTPUT_DIR) : jobConf.get(DEPRECATED_FILE_OUTPUT_FORMAT_OUTPUT_DIR);
            if (paths != null) {
                DataAccessEventProtos.PathEvent pathEvent = DataAccessEventProtos.PathEvent
                        .newBuilder()
                        .setPath(paths)
                        .setType(PathType.OUTPUT.name())
                        .build();
                eventHandler.accept(System.currentTimeMillis(), pathEvent);
            }
        }
    }

}
