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
import org.apache.hadoop.fs.*;
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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileSystemTracerTest {
    private final static Path pathFolder = new Path("/test");
    private static Configuration conf = new Configuration();
    private static String hdfsURI;
    private static MiniDFSCluster hdfsCluster;
    private static Class<?> clazzFS;
    private static Class<?> clazzAFS;
    private static Object dfs;
    private static Object hdfs;
    private static ClassLoader classLoader;
    private static BiConsumer eventHandler;
    private static ArgumentCaptor<DataAccessEventProtos.FsEvent> argument;
    private static String hdfs_user;

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @BeforeClass
    public static void setUpClass() throws IOException, NoSuchFieldException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, URISyntaxException, InstantiationException {
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
                Class.forName("org.apache.hadoop.hdfs.LeaseRenewer$Factory$Key"),
                Class.forName("org.apache.hadoop.fs.Hdfs"),
                HdfsDataOutputStream.class
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
            .simulatedCapacities(new long[] {10240000L});
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort();

        initDFS();
    }

    private static void initDFS() throws ClassNotFoundException, NoSuchMethodException, URISyntaxException, InvocationTargetException, IllegalAccessException, NoSuchFieldException, InstantiationException {
        clazzFS = classLoader.loadClass(DistributedFileSystem.class.getName());

        Method get = clazzFS.getMethod("get", URI.class, Configuration.class);
        dfs = get.invoke(null, new URI(hdfsURI), conf);

        clazzAFS = classLoader.loadClass(Hdfs.class.getName());
        Constructor<?> constructor = clazzAFS.getDeclaredConstructor(URI.class, Configuration.class);
        constructor.setAccessible(true);

        hdfs = constructor.newInstance(new URI(hdfsURI), conf);

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

    private void checkEvent(String action, Path path) {
        checkEvent(action, path, DataAccessEventProtos.FsEvent.Status.SUCCESS);
    }

    private void checkEvent(String action, Path path, DataAccessEventProtos.FsEvent.Status status) {
        assertNotNull(eventHandler);

        verify(eventHandler, atLeastOnce()).accept(any(Long.class), argument.capture());

        DataAccessEventProtos.FsEvent eventTmp = argument.getValue();

        assertEquals(action, eventTmp.getAction());
        assertEquals(hdfsURI, eventTmp.getUri());
        assertEquals(path.toString(), eventTmp.getDstPath());

        assertEquals(hdfs_user, eventTmp.getHdfsUser());

        assertEquals(status, eventTmp.getStatus());
    }

    private void createSrcPathHdfs(Path file) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        Method create = clazzAFS.getMethod("createInternal", Path.class,
            EnumSet.class,
            FsPermission.class,
            int.class,
            short.class,
            long.class,
            Progressable.class,
            Options.ChecksumOpt.class,
            boolean.class);

        //EnumSet<CreateFlag> createFlag
        Object os = create.invoke(hdfs, file, EnumSet.of(CreateFlag.CREATE), FsPermission.getDefault(), 1024, (short) 1, 1048576, null,
            Options.ChecksumOpt.createDisabled(), false);

        Class<?> clazzHdfsDataOutputStream = classLoader.loadClass(HdfsDataOutputStream.class.getName());
        Method write = clazzHdfsDataOutputStream.getMethod("write", byte[].class);
        Method close = clazzHdfsDataOutputStream.getMethod("close", null);
        write.invoke(os, "This is a test".getBytes());
        close.invoke(os);
    }

    private void createSrcPathDistributedFilesystem(Path file) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException, IOException {
        Method create = clazzFS.getMethod("create", Path.class,
            FsPermission.class,
            boolean.class,
            int.class,
            short.class,
            long.class,
            Progressable.class);
        FSDataOutputStream os = (FSDataOutputStream) create.invoke(dfs, file, FsPermission.getDefault(),
            false, 1024, (short) 1, 1048576, null);

        os.write("This is a test".getBytes());
        os.close();
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_write_Hdfs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        Path toWrite = new Path("/test/to_write_dfs");
        createSrcPathHdfs(toWrite);

        checkEvent(FsAction.WRITE.name(), toWrite);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_write_DistributedFileSystem() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, ClassNotFoundException {
        Path toWrite = new Path("/test/to_write_hdfs");
        createSrcPathDistributedFilesystem(toWrite);

        checkEvent(FsAction.WRITE.name(), toWrite);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_read_Hdfs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        Path toRead = new Path("/test/to_read_hdfs");
        createSrcPathHdfs(toRead);

        Method open = clazzAFS.getMethod("open", Path.class,
            int.class);
        open.invoke(hdfs, toRead, 1024);

        checkEvent(FsAction.READ.name(), toRead);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_read_DistributedFileSystem() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, ClassNotFoundException {
        Path toRead = new Path("/test/to_read_dfs");
        createSrcPathDistributedFilesystem(toRead);

        Method open = clazzFS.getMethod("open", Path.class,
            int.class);
        open.invoke(dfs, toRead, 1024);

        checkEvent(FsAction.READ.name(), toRead);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_list_hdfs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method listStatus = clazzAFS.getMethod("listStatus", Path.class);
        listStatus.invoke(hdfs, pathFolder);

        checkEvent(FsAction.LIST_STATUS.name(), pathFolder);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_list_DistributedFileSystem() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method listStatus = clazzFS.getMethod("listStatus", Path.class);
        listStatus.invoke(dfs, pathFolder);

        checkEvent(FsAction.LIST_STATUS.name(), pathFolder);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_get_content() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method getContentSummary = clazzFS.getMethod("getContentSummary", Path.class);
        getContentSummary.invoke(dfs, pathFolder);

        checkEvent(FsAction.GET_CONTENT_SUMMARY.name(), pathFolder);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_delete_Hdfs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        Path toDelete = new Path("/test/to_delete");
        createSrcPathHdfs(toDelete);

        Method delete = clazzAFS.getMethod("delete", Path.class, boolean.class);
        delete.invoke(hdfs, toDelete, true);

        checkEvent(FsAction.DELETE.name(), toDelete);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_delete_DistributedFileSystem() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, ClassNotFoundException {
        Path toDelete = new Path("/test/to_delete");
        createSrcPathDistributedFilesystem(toDelete);

        Method delete = clazzFS.getMethod("delete", Path.class, boolean.class);
        delete.invoke(dfs, toDelete, true);

        checkEvent(FsAction.DELETE.name(), toDelete);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_rename_deprecated() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, ClassNotFoundException {
        Path toRename = new Path("/test/to_rename");
        Path dst = new Path("/test/to_rename_dst_deprecated");
        createSrcPathDistributedFilesystem(toRename);
        Method rename = clazzFS.getMethod("rename", Path.class, Path.class);
        rename.invoke(dfs, toRename, dst);

        checkEvent(FsAction.RENAME.name(), dst);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_rename_Hdfs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        Path toRename = new Path("/test/to_rename");
        Path dst = new Path("/test/to_rename_dst_hdfs");
        createSrcPathHdfs(toRename);

        Method rename = clazzAFS.getMethod("renameInternal", Path.class, Path.class, boolean.class);
        rename.invoke(hdfs, toRename, dst, true);

        checkEvent(FsAction.RENAME.name(), dst);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_attach_to_rename() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, ClassNotFoundException {
        Path toRename = new Path("/test/to_rename");
        Path dst = new Path("/test/to_rename_dst");
        createSrcPathDistributedFilesystem(toRename);
        Method rename = clazzFS.getMethod("rename", Path.class, Path.class, Options.Rename[].class);
        rename.invoke(dfs, toRename, dst, new Options.Rename[0]);

        checkEvent(FsAction.RENAME.name(), dst);
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_indicate_if_event_is_failure_via_exception() throws NoSuchMethodException {
        Method create = clazzFS.getMethod("create", Path.class,
            FsPermission.class,
            boolean.class,
            int.class,
            short.class,
            long.class,
            Progressable.class);


        //Using a uri path with a wrong scheme will force an exception
        try {
            create.invoke(dfs, new Path("ssss://not_a_path"), FsPermission.getDefault(), false, 1024, (short) 1, 1048576, null);
        } catch (Exception ignore) {
        }

        checkEvent(FsAction.WRITE.name(), new Path("ssss://not_a_path"), DataAccessEventProtos.FsEvent.Status.FAILURE);

    }

    @Test
    @AgentAttachmentRule.Enforce
    public void FileSystemTracer_should_indicate_failure_if_method_returns_boolean_false() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Path dst = new Path("/test/to_rename_dst_failure");

        Method rename = clazzFS.getMethod("rename", Path.class, Path.class);
        rename.invoke(dfs, new Path("/not_existing"), dst);

        checkEvent(FsAction.RENAME.name(), dst, DataAccessEventProtos.FsEvent.Status.FAILURE);
    }
}
