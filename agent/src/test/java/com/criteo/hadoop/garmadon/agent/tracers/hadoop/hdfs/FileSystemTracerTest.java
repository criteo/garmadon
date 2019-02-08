package com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs;

import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.agent.tracers.Tracer;
import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.agent.utils.ReflectionHelper;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.BiConsumer;

import static com.criteo.hadoop.garmadon.agent.tracers.hadoop.hdfs.FileSystemTracer.getMethod;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileSystemTracerTest {
    private final Path pathSrc = new Path("/test/garmadon");
    private final Path pathDst = new Path("/test1");
    private final static Path pathFolder = new Path("/test");
    private static Configuration conf = new Configuration();
    private static String hdfsURI;
    private static MiniDFSCluster hdfsCluster;
    private static Class<?> clazzFS;
    private static Object dfs;
    private static ClassLoader classLoader;
    private static BiConsumer eventHandler;
    private static ArgumentCaptor<DataAccessEventProtos.FsEvent> argument;
    private static String hdfs_user;

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @BeforeClass
    public static void setUpClass() throws IOException, NoSuchFieldException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, URISyntaxException {
        classLoader = new ByteArrayClassLoader.ChildFirst(FileSystemTracerTest.class.getClassLoader(),
                ClassFileExtraction.of(
                        Tracer.class,
                        MethodTracer.class,
                        FileSystemTracer.class,
                        FileSystemTracer.WriteTracer.class,
                        FileSystemTracer.ReadTracer.class,
                        FileSystemTracer.AddBlockTracer.class,
                        FileSystemTracer.ListStatusTracer.class,
                        FileSystemTracer.GetContentSummaryTracer.class,
                        FileSystemTracer.RenameTracer.class,
                        FileSystemTracer.DeleteTracer.class,
                        DFSClient.class,
                        DFSClient.Conf.class,
                        ClientContext.class,
                        DistributedFileSystem.class,
                        DomainSocketFactory.class,
                        DFSInputStream.class,
                        DFSOutputStream.class,
                        HdfsDataInputStream.class,
                        HdfsDataOutputStream.class,
                        ClientNamenodeProtocolTranslatorPB.class,
                        Class.forName("org.apache.hadoop.hdfs.DFSOpsCountStatistics"),
                        Class.forName("org.apache.hadoop.hdfs.BlockReaderLocal"),
                        Class.forName(DFSOutputStream.class.getName() + "$Packet"),
                        Class.forName(DFSOutputStream.class.getName() + "$DataStreamer"),
                        Class.forName(DFSOutputStream.class.getName() + "$DataStreamer$1"),
                        Class.forName(DFSOutputStream.class.getName() + "$DataStreamer$2"),
                        Class.forName(DFSOutputStream.class.getName() + "$DataStreamer$ResponseProcessor"),
                        Class.forName(DistributedFileSystem.class.getName() + "$1"),
                        Class.forName(DistributedFileSystem.class.getName() + "$4"),
                        Class.forName(DistributedFileSystem.class.getName() + "$7"),
                        Class.forName(DistributedFileSystem.class.getName() + "$13"),
                        Class.forName(DistributedFileSystem.class.getName() + "$14"),
                        Class.forName(DistributedFileSystem.class.getName() + "$16"),
                        Class.forName(DistributedFileSystem.class.getName() + "$19"),
                        Class.forName("org.apache.hadoop.hdfs.LeaseRenewer"),
                        Class.forName("org.apache.hadoop.hdfs.LeaseRenewer$1"),
                        Class.forName("org.apache.hadoop.hdfs.LeaseRenewer$Factory"),
                        Class.forName("org.apache.hadoop.hdfs.LeaseRenewer$Factory$Key")
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);

        argument = ArgumentCaptor.forClass(DataAccessEventProtos.FsEvent.class);

        eventHandler = mock(BiConsumer.class);

        ReflectionHelper.setField(null, classLoader.loadClass(FileSystemTracer.class.getName()), "eventHandler", eventHandler);
        ReflectionHelper.setField(conf, Configuration.class, "classLoader", classLoader);
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        new FileSystemTracer.WriteTracer().installOnByteBuddyAgent();
        new FileSystemTracer.ReadTracer().installOnByteBuddyAgent();
        new FileSystemTracer.ListStatusTracer().installOnByteBuddyAgent();
        new FileSystemTracer.GetContentSummaryTracer().installOnByteBuddyAgent();
        new FileSystemTracer.DeleteTracer().installOnByteBuddyAgent();
        new FileSystemTracer.RenameTracer().installOnByteBuddyAgent();

        // MiniDfsCluster
        File baseDir = new File("./target/hdfs/test").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                .simulatedCapacities(new long[]{10240000L});
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort();

        initDFS();
    }

    private static void initDFS() throws ClassNotFoundException, NoSuchMethodException, URISyntaxException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        clazzFS = classLoader.loadClass(DistributedFileSystem.class.getName());
        Method get = clazzFS.getMethod("get", URI.class, Configuration.class);
        dfs = get.invoke(null, new URI(hdfsURI), conf);

        Method mkdir = clazzFS.getMethod("mkdir", Path.class, FsPermission.class);
        mkdir.invoke(dfs, pathFolder, FsPermission.getDefault());

        Class clazz = classLoader.loadClass("org.apache.hadoop.hdfs.DistributedFileSystem");
        Field dfsField = clazz.getDeclaredField("dfs");
        dfsField.setAccessible(true);
        Object dfsClient = dfsField.get(dfs);

        Class clazzDFS = classLoader.loadClass("org.apache.hadoop.hdfs.DFSClient");
        Field ugiField = clazzDFS.getDeclaredField("ugi");
        ugiField.setAccessible(true);
        UserGroupInformation ugi = (UserGroupInformation) ugiField.get(dfsClient);

        Class distributedFileSystem = classLoader.loadClass("org.apache.hadoop.security.UserGroupInformation");
        Method getShortUserName = distributedFileSystem.getMethod("getShortUserName");

        hdfs_user = (String) getShortUserName.invoke(ugi);
    }

    private void checkEvent(String action, Path path) throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        assertNotNull(eventHandler);

        verify(eventHandler, atLeastOnce()).accept(any(Long.class), argument.capture());

        DataAccessEventProtos.FsEvent eventTmp = argument.getValue();

        assertEquals(action, eventTmp.getAction());
        assertEquals(hdfsURI, eventTmp.getUri());
        assertEquals(path.toString(), eventTmp.getDstPath());

        assertEquals(hdfs_user, eventTmp.getHdfsUser());
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_write() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
        Method create = clazzFS.getMethod("create", Path.class,
                FsPermission.class,
                boolean.class,
                int.class,
                short.class,
                long.class,
                Progressable.class);
        create.invoke(dfs, pathSrc, FsPermission.getDefault(), false, 1024, (short) 1, 1048576, null);

        checkEvent(FsAction.WRITE.name(), pathSrc);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, NoSuchFieldException, ClassNotFoundException {
        Method create = clazzFS.getMethod("create", Path.class,
                FsPermission.class,
                boolean.class,
                int.class,
                short.class,
                long.class,
                Progressable.class);
        FSDataOutputStream os = (FSDataOutputStream) create.invoke(dfs, pathSrc, FsPermission.getDefault(),
                false, 1024, (short) 1, 1048576, null);

        os.write("This is a test".getBytes());
        os.close();

        Method open = clazzFS.getMethod("open", Path.class,
                int.class);
        open.invoke(dfs, pathSrc, 1024);

        checkEvent(FsAction.READ.name(), pathSrc);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_list() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
        Method listStatus = clazzFS.getMethod("listStatus", Path.class);
        listStatus.invoke(dfs, pathFolder);

        checkEvent(FsAction.LIST_STATUS.name(), pathFolder);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_get_content() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
        Method getContentSummary = clazzFS.getMethod("getContentSummary", Path.class);
        getContentSummary.invoke(dfs, pathFolder);

        checkEvent(FsAction.GET_CONTENT_SUMMARY.name(), pathFolder);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_delete() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
        Method delete = clazzFS.getMethod("delete", Path.class, boolean.class);
        delete.invoke(dfs, pathSrc, true);

        checkEvent(FsAction.DELETE.name(), pathSrc);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_rename() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
        Method rename = clazzFS.getMethod("rename", Path.class, Path.class);
        rename.invoke(dfs, pathSrc, pathDst);

        checkEvent(FsAction.RENAME.name(), pathDst);
    }

}
