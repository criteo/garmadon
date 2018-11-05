package com.criteo.hadoop.garmadon.agent.tracers;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.junit.*;
import org.junit.rules.MethodRule;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class HdfsCallTracerTest {
    private final Path pathFolder = new Path("/test");
    private final Path pathSrc = new Path("/test/garmadon");
    private final Path pathDst = new Path("/test1");
    private static Configuration conf = new Configuration();
    private String hdfsURI;
    private MiniDFSCluster hdfsCluster;
    private Class<?> clazzFS;
    private Object dfs;
    private static ClassLoader classLoader;
    private static Object[] event;


    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @BeforeClass
    public static void setUpClass() throws IOException, NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
        classLoader = new ByteArrayClassLoader.ChildFirst(HdfsCallTracerTest.class.getClassLoader(),
                ClassFileExtraction.of(
                        Tracer.class,
                        MethodTracer.class,
                        HdfsCallTracer.class,
                        HdfsCallTracer.WriteTracer.class,
                        HdfsCallTracer.ReadTracer.class,
                        HdfsCallTracer.AddBlockTracer.class,
                        HdfsCallTracer.ListStatusTracer.class,
                        HdfsCallTracer.GetContentSummaryTracer.class,
                        HdfsCallTracer.RenameTracer.class,
                        HdfsCallTracer.DeleteTracer.class,
                        DFSClient.class,
                        DFSClient.Conf.class,
                        ClientContext.class,
                        DistributedFileSystem.class,
                        DomainSocketFactory.class,
                        DFSOpsCountStatistics.class,
                        DFSInputStream.class,
                        DFSOutputStream.class,
                        HdfsDataInputStream.class,
                        HdfsDataOutputStream.class,
                        ClientNamenodeProtocolTranslatorPB.class,
                        Class.forName("org.apache.hadoop.hdfs.BlockReaderLocal"),
                        Class.forName(HdfsCallTracer.AddBlockTracer.class.getName() + "$SingletonHolder"),
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


        event = new Object[1];
        BiConsumer<Long, Object> cons = (l,o) -> event[0] = o;
        setField(null, classLoader.loadClass(HdfsCallTracer.class.getName()), "eventHandler", cons);
        setField(conf, Configuration.class, "classLoader", classLoader);

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    private static void setField(Object o, Class<?> clazz, String filedName, Object fieldValue) throws NoSuchFieldException, IllegalAccessException {
        Field fieldConf = clazz.getDeclaredField(filedName);
        fieldConf.setAccessible(true);
        fieldConf.set(o, fieldValue);
    }


    @Before
    public void setUp() throws IOException {
        // MiniDfsCluster
        File baseDir = new File("./target/hdfs/test").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort();
    }

    @After
    public void SetDown() {
        hdfsCluster.shutdown();
    }

    private void initDFS() throws ClassNotFoundException, NoSuchMethodException, URISyntaxException, InvocationTargetException, IllegalAccessException {
        clazzFS = classLoader.loadClass(DistributedFileSystem.class.getName());
        Method get = clazzFS.getMethod("get", URI.class, Configuration.class);
        dfs = get.invoke(null, new URI(hdfsURI), conf);

        Method mkdir = clazzFS.getMethod("mkdir", Path.class, FsPermission.class);
        mkdir.invoke(dfs, pathFolder, FsPermission.getDefault());

    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_write() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.WriteTracer().installOnByteBuddyAgent();

        try {
            initDFS();
            Method create = clazzFS.getMethod("create", Path.class,
                    FsPermission.class,
                    boolean.class,
                    int.class,
                    short.class,
                    long.class,
                    Progressable.class);
            create.invoke(dfs, pathSrc, FsPermission.getDefault(), false, 1024, (short) 1, 1048576, null);

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_read() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException, IOException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.ReadTracer().installOnByteBuddyAgent();

        try {
            initDFS();
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

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_list() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException, IOException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.ListStatusTracer().installOnByteBuddyAgent();

        try {
            initDFS();
            Method listStatus = clazzFS.getMethod("listStatus", Path.class);
            listStatus.invoke(dfs, pathFolder);

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_get_content() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException, IOException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.GetContentSummaryTracer().installOnByteBuddyAgent();

        try {
            initDFS();
            Method getContentSummary = clazzFS.getMethod("getContentSummary", Path.class);
            getContentSummary.invoke(dfs, pathFolder);

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_delete() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.DeleteTracer().installOnByteBuddyAgent();

        try {
            initDFS();
            Method delete = clazzFS.getMethod("delete", Path.class, boolean.class);
            delete.invoke(dfs, pathSrc, true);

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    @Test
    @AgentAttachmentRule.Enforce
    public void HdfsCallTracer_should_attach_to_rename() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, URISyntaxException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        ClassFileTransformer classFileTransformer = new HdfsCallTracer.RenameTracer().installOnByteBuddyAgent();

        try {
            initDFS();
            Method rename = clazzFS.getMethod("rename", Path.class, Path.class);
            rename.invoke(dfs, pathSrc, pathDst);

            assertNotNull(event[0]);

        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

}
