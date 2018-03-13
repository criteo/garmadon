package com.criteo.hadoop.garmadon;

// TODO add a flink and a yarn client job
// TODO create a Yarn Mini cluster

import com.criteo.hadoop.garmadon.agent.EventAgent;
import com.criteo.hadoop.garmadon.common.ElasticSearchReader;
import com.criteo.hadoop.garmadon.forwarder.Forwarder;
import com.criteo.hadoop.garmadon.forwarder.metrics.MetricsFactory;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.QuasiMonteCarlo;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.*;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Pattern;

public class EventReaderHandlerIT {
    private static String ES_INDEX = "garmadon-";
    private static String ES_USER = "elastic";
    private static String ES_PASSWORD = "changeme";
    private static String ENCODED = Base64.getEncoder().encodeToString((ES_USER + ":" + ES_PASSWORD).getBytes(StandardCharsets.UTF_8));
    private static Forwarder forwarder;
    private static ElasticSearchReader reader;
    private String tempFolder;
    private JobConf jobConf;

    private Configuration conf;
    private MiniDFSCluster cluster;
    private MiniYARNCluster yarnCluster;
    private String rawHdfsPath;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForService("zookeeper", HealthChecks.toHaveAllPortsOpen())
            .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
            .waitingForService("elasticsearch", HealthChecks.toHaveAllPortsOpen())
            //.waitingForService("elasticsearch", HealthChecks.toRespond2xxOverHttp(9200,
            //        (port) -> port.inFormat("http://$HOST:$EXTERNAL_PORT")))
            .removeConflictingContainersOnStartup(true)
            // This slowdown startup
            //.pullOnStartup(true)
            .build();

    @BeforeClass
    public static void setUp() throws IOException {
        // Init ES Reader
        //System.setProperty("garmadon.esReader.printToStdout", "true");
        System.setProperty("garmadon.esReader.bulkActions", "1");
        System.setProperty("garmadon.esReader.bulkFlushIntervalSec", "1");

        createEsPipeline();

        Properties properties = new Properties();
        properties.put("es.user", ES_USER);
        properties.put("es.password", ES_PASSWORD);
        properties.put("es.host", "localhost");
        properties.put("es.port", "9200");
        properties.put("es.index", ES_INDEX);
        properties.put("kafka.connection", "localhost:9092");
        reader = new ElasticSearchReader(properties);
        reader.startReading();

        // Init forwarder
        try (InputStream streamPropFilePath = Forwarder.class.getResourceAsStream("/server.properties")) {
            properties = new Properties();
            properties.load(streamPropFilePath);

            forwarder = new Forwarder(properties);
            forwarder.run();
        }

        // Init Java Agent
        Instrumentation instr = ByteBuddyAgent.install();
        EventAgent.premain("com.criteo.hadoop.garmadon.agent.modules.ContainerModules", instr);

    }

    @AfterClass
    public static void tearDown() throws IOException, InterruptedException {
        // JVM Metrics
        Assert.assertTrue(0 < reader.searchEs(QueryBuilders.existsQuery("collector_name")));
        // Forwarder Metrics
        Assert.assertTrue(0 < reader.searchEs(QueryBuilders.existsQuery("events-received")));
        Assert.assertTrue(0 < reader.searchEs(QueryBuilders.matchQuery("tag", Header.Tag.FORWARDER.toString())));

        // Ensure all events have well been pushed to Kafka/ES
        Assert.assertTrue(MetricsFactory.eventsReceived.getCount() < reader.searchEs(QueryBuilders.matchAllQuery()));

        if (forwarder != null)
            forwarder.close();

        flushEs();

        //make this test here as it is the only moment where the connection
        //will be closed with the forwarder

        // StateEndEvent
        Assert.assertEquals(1, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("state", "end"))));

        if (reader != null)
            reader.stop().join();
    }

    @Before
    public void setUpEach() throws IOException {
        // Init HDFS Mini cluster
        tempFolder = Files.createTempDirectory("temp").toString();
        miniHdfsCluster(tempFolder);
        miniYarnCluster();

        setEnv("LOCAL_DIRS", conf.get("yarn.nodemanager.local-dirs"));
        jobConf = new JobConf(conf);
    }

    @After
    public void tearDownEach() {
        if (cluster != null)
            cluster.shutdown(true);
        if (yarnCluster != null)
            yarnCluster.stop();
    }

    private static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    private void miniHdfsCluster(String rootPath) throws IOException {
        // Init HDFS Mini Cluster
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();
        File testDataCluster1 = new File(rootPath, "CLUSTER_1");
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        cluster = new MiniDFSCluster.Builder(conf).build();

        // Copy local file to HDFS cluster
        FileSystem fs = FileSystem.get(conf);
        Path homeDir = fs.getHomeDirectory();
        String inputRawData = Forwarder.class.getResource("/docker-compose.yml").getFile();
        Path inputData = new Path(inputRawData);
        rawHdfsPath = homeDir + "/testing/input/data.txt";
        Path rawHdfsData = new Path(rawHdfsPath);
        fs.copyFromLocalFile(inputData, rawHdfsData);
    }

    private void miniYarnCluster() {
        conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
        conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
        yarnCluster = new MiniYARNCluster("YarnCluster", 1, 1, 1);
        yarnCluster.init(conf);
        yarnCluster.start();
    }

    private static void createEsPipeline() throws IOException {
        URL obj = new URL("http://localhost:9200/_ingest/pipeline/dailyindex");
        String json = "{" +
                "\"description\": \"daily date-time index naming\",\n" +
                "  \"processors\" : [\n" +
                "    {\n" +
                "      \"date_index_name\" : {\n" +
                "        \"field\" : \"timestamp\",\n" +
                "        \"index_name_prefix\" : \"" + ES_INDEX + "\",\n" +
                "        \"date_rounding\" : \"d\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]" +
                "}";

        HttpURLConnection con = null;

        try {
            con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("PUT");
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            con.setRequestProperty("Authorization", "Basic " + ENCODED);

            OutputStreamWriter osw = new OutputStreamWriter(con.getOutputStream());
            osw.write(json);
            osw.flush();
            osw.close();

            if (con.getResponseCode() != 200)
                throw new IOException("Issue creating ES pipeline: " + con.getResponseCode() + " :" + con.getResponseMessage());
        } finally {
            if (con != null)
                con.disconnect();

        }
    }

    private static void flushEs() throws IOException, InterruptedException {
        Thread.sleep(2000L);
        URL obj = new URL("http://localhost:9200/_flush");
        HttpURLConnection con = null;
        try {
            con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Authorization", "Basic " + ENCODED);

            OutputStreamWriter osw = new OutputStreamWriter(con.getOutputStream());
            osw.flush();
            osw.close();
            if (con.getResponseCode() != 200)
                throw new IOException("Issue flushing ES: " + con.getResponseCode() + " :" + con.getResponseMessage());
        } finally {
            if (con != null)
                con.disconnect();

        }
    }

    @Test
    public void EventServerHandler_should_run_MR_TeraGen() throws Exception {
        File tmpFolder = new File(new File(tempFolder), Long.toString(System.nanoTime()));
        tmpFolder.deleteOnExit();

        String[] args = {"100000", "/test/teragen"};
        TeraGen teraGen = new TeraGen();
        teraGen.setConf(jobConf);
        teraGen.run(args);

        flushEs();
        // OutputPath Event
        Assert.assertEquals(1, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("path", "/test/teragen"))
                .must(QueryBuilders.matchQuery("type", "OUTPUT"))));
        // FsEvent WRITE
        Assert.assertEquals(2, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "/test/teragen"))
                .must(QueryBuilders.matchQuery("action", "WRITE"))));

    }

    @Test
    public void EventServerHandler_should_run_MR_PI() throws Exception {
        QuasiMonteCarlo qmc = new QuasiMonteCarlo();
        String[] args = {"5", "100000000"};
        qmc.setConf(jobConf);
        qmc.run(args);

        flushEs();
        // InputPathEvent
        Assert.assertEquals(6, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("path", "in"))
                .must(QueryBuilders.matchQuery("type", "INPUT"))));
        // FsEvent READ
        Assert.assertEquals(5, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "in"))
                .must(QueryBuilders.matchQuery("action", "READ"))));
        // FsEvent WRITE
        Assert.assertEquals(3, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "out"))
                .must(QueryBuilders.matchQuery("action", "WRITE"))));
        // FsEvent RENAME
        Assert.assertEquals(2, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "out"))
                .must(QueryBuilders.matchQuery("action", "RENAME"))));
        // FsEvent DELETE
        Assert.assertEquals(1, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "out"))
                .must(QueryBuilders.matchQuery("action", "DELETE"))));
    }

    @Test
    public void EventServerHandler_should_run_Spark_PI() throws IOException, InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaSparkPi")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map((Function<Integer, Integer>) integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        }).reduce((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        spark.stop();

        flushEs();
    }

    @Test
    public void EventServerHandler_should_run_Spark_JavaWord() throws IOException, InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(rawHdfsPath).javaRDD();

        Pattern SPACE = Pattern.compile(" ");
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();

        flushEs();
        // FsEvent READ
        Assert.assertEquals(1, reader.searchEs(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("dst_path", "data.txt"))
                .must(QueryBuilders.matchQuery("action", "READ"))));
    }

}
