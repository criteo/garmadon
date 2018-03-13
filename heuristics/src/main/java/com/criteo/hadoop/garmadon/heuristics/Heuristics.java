package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.schema.events.StateEvent;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.criteo.jvm.JVMStatisticsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.ExportException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

public class Heuristics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);
    private static final GarmadonMessageFilter JVM_FILTER = new JVMFilter();

    private static JMXConnectorServer jmxServer;

    private final GarmadonReader reader;
    private final List<GCStatsHeuristic> gcStatsHeuristics = new CopyOnWriteArrayList<>();
    private final List<JVMStatsHeuristic> jvmStatsHeuristics = new CopyOnWriteArrayList<>();
    private final HeuristicsResultDB heuristicsResultDB;
    private long gcEventCount;
    private long jvmEventCount;
    private long stateEventCount;

    public Heuristics(String kafkaConnectString, String dreConnectionString, String heuristicPropFileName) {
        GarmadonReader.Builder builder = GarmadonReader.Builder.withKafkaConnectString(kafkaConnectString);
        reader = builder
                .listeningToMessages(JVM_FILTER, this::process)
                //.listeningToMessages(JVM_FILTER, this::printToStdout)
                .build();
        this.heuristicsResultDB = new HeuristicsResultDB(dreConnectionString);
        // Hard coded heuristic registration
        registerHeuristicProp(GCStatsHeuristic.CATEGORY_NAME, GCCause.class.getName());
        registerHeuristicProp(GCStatsHeuristic.CATEGORY_NAME, G1GC.class.getName());
        registerHeuristicProp(JVMStatsHeuristic.CATEGORY_NAME, Threads.class.getName());
        registerHeuristicProp(JVMStatsHeuristic.CATEGORY_NAME, HeapUsage.class.getName());
        registerHeuristicProp(JVMStatsHeuristic.CATEGORY_NAME, CodeCacheUsage.class.getName());
        registerHeuristicProp(JVMStatsHeuristic.CATEGORY_NAME, Safepoints.class.getName());
        registerHeuristicProp(JVMStatsHeuristic.CATEGORY_NAME, Locks.class.getName());
        loadHeuristicPropFile(heuristicPropFileName);
    }

    private void loadHeuristicPropFile(String heuristicPropFileName) {
        if (heuristicPropFileName == null)
            return;
        File heuristicPropFile = new File(heuristicPropFileName);
        if (!heuristicPropFile.exists()) {
            LOGGER.warn("Cannot found heuristic property file {}", heuristicPropFile.getAbsolutePath());
            return;
        }
        Properties props = new Properties();
        try {
            try (Reader propReader = new FileReader(heuristicPropFile)) {
                props.load(propReader);
                props.forEach(this::registerHeuristicProp);
            }
        } catch (IOException ex) {
            LOGGER.error("Error reading heuristic property file {}", heuristicPropFile.getAbsolutePath(), ex);
        }
    }

    private void registerHeuristicProp(Object category, Object className) {
        try {
            Class<?> clazz = Class.forName((String) className);
            Constructor<?> constructor = clazz.getDeclaredConstructor(HeuristicsResultDB.class);
            Object instance = constructor.newInstance(heuristicsResultDB);
            String categoryName = (String) category;
            switch (categoryName) {
                case GCStatsHeuristic.CATEGORY_NAME:
                    gcStatsHeuristics.add((GCStatsHeuristic)instance);
                    break;
                case JVMStatsHeuristic.CATEGORY_NAME:
                    jvmStatsHeuristics.add((JVMStatsHeuristic)instance);
                    break;
                default:
                    LOGGER.warn("Unknown category {}", categoryName);
            }
        } catch (Exception ex) {
            LOGGER.warn("Cannot register heuristic {} {}", category, className, ex);
        }
    }

    public void start() {
        reader.startReading().whenComplete(this::completeReading);
    }

    private void completeReading(Void dummy, Throwable ex) {
        if (ex != null) {
            LOGGER.error("Reading was stopped due to exception");
            ex.printStackTrace();
        } else {
            LOGGER.info("Done reading !");
        }
    }

    public void stop() {
        reader.stopReading().whenComplete(this::completeReading);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            printHelp();
            return;
        }
        String kafkaConnectString = args[0];
        String dreConnectionString = args[1];
        String heuristicPropFileName = args.length >= 3 ? args[2] : null;
        Heuristics heuristics = new Heuristics(kafkaConnectString, dreConnectionString, heuristicPropFileName);
        heuristics.start();
        Runtime.getRuntime().addShutdownHook(new Thread(heuristics::stop));
        jmxServer = createJmxConnectorServer(18000);
        registerMXBean();
    }

    private void process(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        String containerId = msg.getHeader().getContainerId();
        switch (msg.getType()) {
            case GarmadonSerialization.TypeMarker.GC_EVENT:
                gcEventCount++;
                JVMStatisticsProtos.GCStatisticsData gcStats = (JVMStatisticsProtos.GCStatisticsData) msg.getBody();
                gcStatsHeuristics.forEach(h -> h.process(applicationId, containerId, gcStats));
                break;
            case GarmadonSerialization.TypeMarker.JVMSTATS_EVENT:
                if (msg.getHeader().getTag().equals("FORWARDER")) {

                } else {
                    jvmEventCount++;
                    JVMStatisticsProtos.JVMStatisticsData jvmStats = (JVMStatisticsProtos.JVMStatisticsData) msg.getBody();
                    jvmStatsHeuristics.forEach(h -> h.process(applicationId, containerId, jvmStats));
                }
                break;
            case GarmadonSerialization.TypeMarker.STATE_EVENT:
                stateEventCount++;
                DataAccessEventProtos.StateEvent stateEvent = (DataAccessEventProtos.StateEvent) msg.getBody();
                if (StateEvent.State.END.toString().equals(stateEvent.getState())) {
                    gcStatsHeuristics.forEach(h -> h.onCompleted(applicationId, containerId));
                    jvmStatsHeuristics.forEach(h -> h.onCompleted(applicationId, containerId));
                }
                break;
            default:
                LOGGER.error("Unknown type marker: " + msg.getType());
        }
        // TODO handle safety net for APP_END_EVENT => compute the normal interval based on timestamp
        // TODO => if no msg for an appId after 2/3*interval (still based on timestamp) => trigger APP_END
        if ((jvmEventCount+gcEventCount+stateEventCount) % 1000 == 0) {
            LOGGER.info("GC: {} JVM: {} STATE: {}", gcEventCount, jvmEventCount, stateEventCount);
            //msg.getCommittableOffset().commitAsync();
        }
    }

    private void printToStdout(GarmadonMessage msg) {
        System.out.print(msg.getHeader().getApplicationId());
        System.out.print("|");
        System.out.print(msg.getHeader().getApplicationName());
        System.out.print("|");
        System.out.print(msg.getHeader().getContainerId());
        System.out.print("|");
        System.out.print(msg.getHeader().getHostname());
        System.out.print("|");
        System.out.print(msg.getHeader().getUserName());
        System.out.print("|");
        System.out.print(msg.getBody().toString().replaceAll("\\n", " - "));
        System.out.println();
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.heuristics.Heuristics <kafkaConnectionString> <DrElephantConnectionString> [heuristicPorpertyFile]");
    }

    private static JMXConnectorServer createJmxConnectorServer(int jmxPort) {
        if (System.getProperty("com.sun.management.jmxremote") != null) {
            LOGGER.info("Remote JMX already enabled");
            return null;
        }
        try {
            int offset = 0;
            do {
                try {
                    LocateRegistry.createRegistry(jmxPort + offset);
                    LOGGER.info("JMX remote server bound to {}", jmxPort + offset);
                    break;
                } catch (ExportException ex) {
                    if (ex.getCause() instanceof BindException) {
                        LOGGER.warn("Cannot bind: {}, trying another port...", ex.toString());
                        offset++;
                    } else
                        throw ex;
                }
            } while (offset < 100);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:" + jmxPort + "/jmxrmi");
            JMXConnectorServer svr = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
            svr.start();
            return svr;
        } catch (IOException ex) {
            LOGGER.warn("Error creating remote JMX server", ex);
            return null;
        }
    }

    private static void registerMXBean() {
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        try
        {
            platformMBeanServer.registerMBean(new HeuristicsMXBeanImpl(), new ObjectName("com.criteo.hadoop.garmadon.HeuristicsManagement:type=Heuristics"));
        }
        catch (Exception ex)
        {
            LOGGER.warn("Error registering MXBean", ex);
        }
    }

    private static class JVMFilter implements GarmadonMessageFilter {

        @Override
        public boolean acceptsType(int type) {
            return type == GarmadonSerialization.TypeMarker.GC_EVENT
                    || type == GarmadonSerialization.TypeMarker.JVMSTATS_EVENT
                    || type == GarmadonSerialization.TypeMarker.STATE_EVENT;
        }

        @Override
        public boolean acceptsHeader(DataAccessEventProtos.Header header) {
            return true;
        }
    }
}
