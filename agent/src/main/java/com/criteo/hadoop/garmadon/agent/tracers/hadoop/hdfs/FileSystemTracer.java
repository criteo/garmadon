package com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs;

import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Progressable;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class FileSystemTracer {
    private static long NANOSECONDS_PER_MILLISECOND = 1000000;
    private static BiConsumer<Long, Object> eventHandler;

    public static void setup(Instrumentation instrumentation, BiConsumer<Long, Object> eventConsumer) {

        initEventHandler(eventConsumer);

        new FileSystemTracer.ReadTracer().installOn(instrumentation);
        new FileSystemTracer.WriteTracer().installOn(instrumentation);
        new FileSystemTracer.RenameTracer().installOn(instrumentation);
        new FileSystemTracer.DeleteTracer().installOn(instrumentation);
        new FileSystemTracer.AppendTracer().installOn(instrumentation);
        new FileSystemTracer.ListStatusTracer().installOn(instrumentation);
        new FileSystemTracer.GetContentSummaryTracer().installOn(instrumentation);
        new FileSystemTracer.AddBlockTracer().installOn(instrumentation);
    }

    public static void initEventHandler(BiConsumer<Long, Object> eventConsumer) {
        FileSystemTracer.eventHandler = eventConsumer;
    }

    public static class DeleteTracer extends MethodTracer {

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
            return to(FileSystemTracer.DeleteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.DELETE.name());
        }
    }

    public static class ReadTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("open").and(takesArguments(Path.class, int.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.ReadTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.READ.name());
        }
    }

    public static class RenameTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("rename");
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.RenameTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path src,
                @Argument(1) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), src.toString(), dst.toString(), FsAction.RENAME.name());
        }
    }

    public static class WriteTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
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
        protected Implementation newImplementation() {
            return to(FileSystemTracer.WriteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.WRITE.name());
        }
    }

    public static class AppendTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("append").and(
                    takesArguments(
                            Path.class,
                            int.class,
                            Progressable.class
                    )
            );
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.AppendTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.APPEND.name());
        }
    }

    public static class ListStatusTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("listStatus").and(
                    takesArguments(
                            Path.class
                    )
            );
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.ListStatusTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.LIST_STATUS.name());
        }
    }

    public static class GetContentSummaryTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getContentSummary").and(
                    takesArguments(
                            Path.class
                    )
            );
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.GetContentSummaryTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Path dst) throws Exception {
            Object uri = ((DistributedFileSystem) o).getUri();
            return executeMethod(zuper, uri.toString(), null, dst.toString(), FsAction.GET_CONTENT_SUMMARY.name());
        }
    }

    public static class AddBlockTracer extends MethodTracer {
        /**
         * We have to load the class ClientNamenodeProtocolTranslatorPB after instrumenting it
         * If we set it directly in a static in AddBlockTracer it will load the class
         * before instrumenting it
         * With Singleton mechanism we ensure load of class only when accesing to getField method
         */
        private static class SingletonHolder {
            private static Field field;

            static {
                try {
                    field = ClientNamenodeProtocolTranslatorPB.class.getDeclaredField("rpcProxy");
                    field.setAccessible(true);
                } catch (Exception ignore) {
                }
            }
        }

        static Field getField() {
            return FileSystemTracer.AddBlockTracer.SingletonHolder.field;
        }


        @Override
        public ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("addBlock").and(takesArguments(String.class, String.class, ExtendedBlock.class,
                    DatanodeInfo[].class, long.class, String[].class, EnumSet.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.AddBlockTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) String dst) throws Exception {
            if (getField() != null) {
                ClientNamenodeProtocolPB rpcProxy = (ClientNamenodeProtocolPB) getField().get(o);
                return executeMethod(zuper, "hdfs://" + RPC.getServerAddress(rpcProxy).getHostString() + ":" + RPC.getServerAddress(rpcProxy).getPort(),
                        null, dst, FsAction.ADD_BLOCK.name());
            } else {
                return zuper.call();
            }
        }
    }

    private static Object executeMethod(@SuperCall Callable<?> zuper, String uri, String src, String dst, String fsAction) throws Exception {
        long startTime = System.nanoTime();
        try {
            return zuper.call();
        } catch (Exception e) {
            throw e;
        } finally {
            long elapsedTime = (System.nanoTime() - startTime) / NANOSECONDS_PER_MILLISECOND;
            sendFsEvent(uri, src, dst, fsAction, elapsedTime);
        }
    }

    private static void sendFsEvent(String uri, String src, String dst, String fsAction, long durationMillis) {
        DataAccessEventProtos.FsEvent.Builder eventBuilder = DataAccessEventProtos.FsEvent
                .newBuilder();

        eventBuilder.setAction(fsAction)
                .setDstPath(dst)
                .setUri(uri)
                .setMethodDurationMillis(durationMillis);

        if (src != null) {
            eventBuilder.setSrcPath(src);
        }

        eventHandler.accept(System.currentTimeMillis(), eventBuilder.build());

    }
}
