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
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatcher;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class FileSystemTracer {
    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;
    private static final ConcurrentHashMap METHOD_GET_URI = new ConcurrentHashMap<ClassLoader, Method>();
    private static final ConcurrentHashMap METHOD_GET_SERVER_ADDRESS = new ConcurrentHashMap<ClassLoader, Method>();
    private static final ConcurrentHashMap FIELD_RPC_PROXY = new ConcurrentHashMap<ClassLoader, Field>();
    private static BiConsumer<Long, Object> eventHandler;

    private static TypeDescription pathTD =
            new TypeDescription.Latent("org.apache.hadoop.fs.Path", Opcodes.ACC_PUBLIC, TypeDescription.Generic.OBJECT);

    protected FileSystemTracer() {
        throw new UnsupportedOperationException();
    }

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

    public static ConcurrentHashMap getMethodGetServerAddress() {
        return METHOD_GET_SERVER_ADDRESS;
    }

    public static ConcurrentHashMap getFieldRpcProxy() {
        return FIELD_RPC_PROXY;
    }

    public static ConcurrentHashMap getMethodGetUri() {
        return METHOD_GET_URI;
    }

    public static Method getMethod(ClassLoader classLoader, String clazz, String method, Class<?>... parameterTypes) {
        try {
            Class distributedFileSystem = classLoader.loadClass(clazz);
            return distributedFileSystem.getMethod(method, parameterTypes);
        } catch (NoSuchMethodException | ClassNotFoundException ignored) {
            return null;
        }
    }

    public static void initEventHandler(BiConsumer<Long, Object> eventConsumer) {
        FileSystemTracer.eventHandler = eventConsumer;
    }

    public static class DeleteTracer extends MethodTracer {


        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("delete").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.DeleteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.DELETE.name());
        }
    }

    public static class ReadTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("open").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.ReadTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.READ.name());
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
                @Argument(0) Object src,
                @Argument(1) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), src.toString(), dst.toString(), FsAction.RENAME.name());
        }
    }

    public static class WriteTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("create").and(takesArguments(7)).and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.WriteTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.WRITE.name());
        }
    }

    public static class AppendTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("append").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.AppendTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.APPEND.name());
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
                    takesArguments(pathTD)
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
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.LIST_STATUS.name());
        }
    }

    public static class GetContentSummaryTracer extends MethodTracer {
        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.DistributedFileSystem");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("getContentSummary").and(takesArgument(0, pathTD));
        }

        @Override
        protected Implementation newImplementation() {
            return to(FileSystemTracer.GetContentSummaryTracer.class);
        }

        @RuntimeType
        public static Object intercept(
                @SuperCall Callable<?> zuper,
                @This Object o,
                @Argument(0) Object dst) throws Exception {
            ClassLoader classLoader = o.getClass().getClassLoader();
            Method getUri = (Method) getMethodGetUri().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.hdfs.DistributedFileSystem", "getUri"));
            return executeMethod(zuper, getUri.invoke(o).toString(), null, dst.toString(), FsAction.GET_CONTENT_SUMMARY.name());
        }
    }

    public static class AddBlockTracer extends MethodTracer {
        @Override
        public ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("addBlock").and(takesArgument(0, String.class));
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

            ClassLoader classLoader = o.getClass().getClassLoader();
            Field field = (Field) getFieldRpcProxy().computeIfAbsent(classLoader, k -> {
                try {
                    Class clientNamenodeProtocolTranslatorPB = classLoader.loadClass("org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB");
                    Field fieldComputed = clientNamenodeProtocolTranslatorPB.getDeclaredField("rpcProxy");
                    fieldComputed.setAccessible(true);
                    return fieldComputed;
                } catch (ClassNotFoundException | NoSuchFieldException ignored) {
                    return null;
                }
            });
            Object rpcProxy = field.get(o);

            Method getServerAddress = (Method) getMethodGetServerAddress().computeIfAbsent(classLoader,
                    k -> getMethod(classLoader, "org.apache.hadoop.ipc.RPC", "getServerAddress", Object.class));
            InetSocketAddress inetSocketAddress = (InetSocketAddress) getServerAddress.invoke(o, rpcProxy);
            return executeMethod(zuper, "hdfs://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort(),
                    null, dst, FsAction.ADD_BLOCK.name());
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
