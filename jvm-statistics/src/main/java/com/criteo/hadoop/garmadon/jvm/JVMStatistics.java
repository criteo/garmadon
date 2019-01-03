package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.jvm.statistics.HotSpotMXBeanStatistics;
import com.criteo.hadoop.garmadon.jvm.statistics.MXBeanStatistics;
import com.criteo.hadoop.garmadon.jvm.statistics.MachineStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class JVMStatistics {

    public static final String DEFAULT_SINK_TYPE = "log";
    private static final Logger LOGGER = LoggerFactory.getLogger(JVMStatistics.class);

    private static final Map<String, Class<?>> STATISTIC_COLLECTOR_CLASSES = new ConcurrentHashMap<>();
    private static final Map<String, Class<?>> GC_NOTIFICATIONS_CLASSES = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, JVMStatistics::createDaemonThread);
    private final Conf<?, ?, ?> conf;
    private final StatisticCollector<?> jvmStatsCollector;
    private final StatisticCollector<?> machineStatsCollector;
    private final GCNotifications gcNotifications;

    public JVMStatistics(Conf<?, ?, ?> conf) {
        this.conf = conf;
        this.jvmStatsCollector = createStatisticCollector(conf, conf.getLogJVMStats());
        this.machineStatsCollector = createStatisticCollector(conf, conf.getLogMachineStats());
        this.gcNotifications = createGCNotifications(conf);
    }

    static {
        registerStatisticCollector(DEFAULT_SINK_TYPE, LogStatisticCollector.class);
        registerGCNotifications(DEFAULT_SINK_TYPE, LogGCNotifications.class);
    }

    public static void registerStatisticCollector(String name, Class<?> clazz) {
        STATISTIC_COLLECTOR_CLASSES.put(name, clazz);
    }

    public static void registerGCNotifications(String name, Class<?> clazz) {
        GC_NOTIFICATIONS_CLASSES.put(name, clazz);
    }

    public void start() {
        try {
            registerStatistics();
        } catch (Exception ex) {
            LOGGER.debug("Cannot register statistics", ex);
        }
        executor.scheduleWithFixedDelay(this::collect, 0, conf.getInterval().getSeconds(), TimeUnit.SECONDS);
    }

    public void delayStart(int seconds) {
        executor.schedule(this::start, seconds, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdownNow();
        jvmStatsCollector.unregister();
        gcNotifications.unsubscribe();
    }

    private void collect() {
        jvmStatsCollector.collect();
        machineStatsCollector.collect();
    }

    private static StatisticCollector<?> createStatisticCollector(Conf conf, BiConsumer<Long, ?> logStatistics) {
        Class<?> clazz = STATISTIC_COLLECTOR_CLASSES.get(conf.getSinkType());
        if (clazz == null) {
            throw new UnsupportedOperationException(conf.getSinkType() + " statistics collector not supported");
        }
        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(BiConsumer.class);
            return (StatisticCollector<?>) constructor.newInstance(logStatistics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private GCNotifications createGCNotifications(Conf<?, ?, ?> conf) {
        Class<?> clazz = GC_NOTIFICATIONS_CLASSES.get(conf.getSinkType());
        if (clazz == null) {
            throw new UnsupportedOperationException(conf.getSinkType() + " gc notifications not supported");
        }
        try {
            return (GCNotifications) clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void registerStatistics() {
        if (conf.isJvmStatsEnabled()) {
            MXBeanStatistics mxBeanStatistics;
            try {
                mxBeanStatistics = new HotSpotMXBeanStatistics(conf);
            } catch (ClassNotFoundException ex) {
                mxBeanStatistics = new MXBeanStatistics(conf);
            }
            mxBeanStatistics.register(jvmStatsCollector);
        }
        if (conf.isGcStatsEnabled()) {
            gcNotifications.subscribe(conf.getLogGcStats());
        }
        if (conf.isMachineStatsEnabled()) {
            MachineStatistics machineStatistics = new MachineStatistics();
            machineStatistics.register(machineStatsCollector);
        }
    }

    /**
     * Used to test JVM stats on a specific environment
     */
    public static void main(String[] args) {
        Conf<String, String, String> conf = new Conf<>();
        conf.setInterval(Duration.ofSeconds(1));
        conf.setLogJVMStats(JVMStatistics::log);
        conf.setLogGcStats(JVMStatistics::log);
        JVMStatistics stats = new JVMStatistics(conf);
        stats.start();
        Thread t = new Thread(() -> {
            int i = 0;
            String s;
            while (true) {
                s = new Integer(i++).toString();
                Thread.yield();
            }
        });
        t.setDaemon(true);
        t.start();

        LockSupport.parkNanos(TimeUnit.MINUTES.toNanos(5));
        stats.stop();
    }

    private static void log(Long timestamp, String str) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
        System.out.println(LocalDateTime.now().format(formatter) + " " + str);
    }

    private static Thread createDaemonThread(Runnable r) {
        Thread t = new Thread(r, "JVMStatistics");
        t.setDaemon(true);
        return t;
    }
}
