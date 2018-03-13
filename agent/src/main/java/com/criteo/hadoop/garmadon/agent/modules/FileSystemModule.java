package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

import java.lang.instrument.Instrumentation;
import java.util.function.Consumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class FileSystemModule extends ContainerModule {

    @Override
    public void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer) {
        new ReadTracer(eventConsumer::accept).installOn(instrumentation);
        new WriteTracer(eventConsumer::accept).installOn(instrumentation);
        new RenameTracer(eventConsumer::accept).installOn(instrumentation);
        new DeleteTracer(eventConsumer::accept).installOn(instrumentation);
    }

    public static class DeleteTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public DeleteTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        public ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("delete").and(takesArguments(Path.class, boolean.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            FsEvent event = new FsEvent(System.currentTimeMillis(), dst.toString(), FsEvent.Action.DELETE, uri.toString());
            eventHandler.accept(event);
        }

    }

    public static class ReadTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public ReadTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("open").and(takesArguments(Path.class, int.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(
                @This Object o,
                @Argument(0) Path f) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            FsEvent event = new FsEvent(System.currentTimeMillis(), f.toString(), FsEvent.Action.READ, uri.toString());
            eventHandler.accept(event);
        }
    }

    public static class RenameTracer extends MethodTracer {

        final Consumer<Object> eventHandler;

        public RenameTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("rename");
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(
                @This Object o,
                @Argument(0) Path src,
                @Argument(1) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            FsEvent event = new FsEvent(System.currentTimeMillis(), src.toString(), dst.toString(), FsEvent.Action.RENAME, uri.toString());
            eventHandler.accept(event);
        }
    }

    public static class WriteTracer extends MethodTracer {

        private final Consumer<Object> eventHandler;

        public WriteTracer(Consumer<Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("create").and(
                    takesArguments(
                            Path.class,
                            FsPermission.class,
                            boolean.class,
                            int.class,
                            short.class,
                            long.class,
                            Progressable.class
                    ));
        }

        @Override
        Implementation newImplementation() {
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@This Object o, @Argument(0) Path f) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            FsEvent event = new FsEvent(System.currentTimeMillis(), f.toString(), FsEvent.Action.WRITE, uri.toString());
            eventHandler.accept(event);
        }
    }
}
