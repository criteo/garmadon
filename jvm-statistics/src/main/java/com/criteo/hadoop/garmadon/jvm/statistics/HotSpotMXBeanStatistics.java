package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.Conf;
import com.criteo.hadoop.garmadon.jvm.StatisticCollector;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.UnixOperatingSystemMXBean;
import sun.management.ManagementFactoryHelper;

import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HotSpotMXBeanStatistics extends MXBeanStatistics {

    private static final String OS_NAME = System.getProperty("os.name") != null ? System.getProperty("os.name") : "";

    public HotSpotMXBeanStatistics(Conf conf) throws ClassNotFoundException {
        super(conf);
        Class.forName("sun.management.ManagementFactoryHelper");
    }

    @Override
    public void register(StatisticCollector collector) {
        super.register(collector);
        addProcessStatistics(collector);
        addSafepointStatistics(collector);
        addSynchronizationStatistics(collector);
        addFileDescriptorStatistics(collector);
    }

    @Override
    protected void addCompilationStatistics(StatisticCollector collector) {
        try {
            collector.register(createCompilationStatistics());
        } catch (Throwable ex) {
            collector.delayRegister(this::createCompilationStatistics);
        }
    }

    private AbstractStatistic createCompilationStatistics() {
        return new HotspotCompilationStatistics(ManagementFactory.getCompilationMXBean(), ManagementFactoryHelper.getHotspotCompilationMBean());
    }

    @Override
    protected void addThreadStatistics(StatisticCollector collector) {
        try {
            collector.register(createThreadStatistics());
        } catch (Throwable ex) {
            collector.delayRegister(this::createThreadStatistics);
        }
    }

    private AbstractStatistic createThreadStatistics() {
        return new HotspotThreadStatistics(ManagementFactory.getThreadMXBean(), ManagementFactoryHelper.getHotspotThreadMBean());
    }

    @Override
    protected void addClassStatistics(StatisticCollector collector) {
        try {
            collector.register(createClassStatistics());
        } catch (Throwable ex) {
            collector.delayRegister(this::createClassStatistics);
        }
    }

    private AbstractStatistic createClassStatistics() {
        return new HotspotClassStatistics(ManagementFactory.getClassLoadingMXBean(), ManagementFactoryHelper.getHotspotClassLoadingMBean());
    }

    @Override
    protected void addOSStatistics(StatisticCollector collector) {
        try {
            AbstractStatistic osStatistics = createOSStatistics();
            if (osStatistics != null) {
                collector.register(osStatistics);
            }
        } catch (Throwable ex) {
            collector.delayRegister(this::createOSStatistics);
        }
        try {
            AbstractStatistic cpuStatistics = createCpuStatistics();
            if (cpuStatistics != null) {
                collector.register(cpuStatistics);
                return;
            }
        } catch (Throwable ex) {
            collector.delayRegister(this::createCpuStatistics);
            return;
        }
        super.addOSStatistics(collector);
    }

    private AbstractStatistic createOSStatistics() {
        java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        int processors = os.getAvailableProcessors();
        OperatingSystemMXBean hsOs = null;
        if (os instanceof OperatingSystemMXBean) {
            hsOs = (OperatingSystemMXBean) os;
            if ("Linux".equals(OS_NAME)) hsOs = new LinuxMemInfoWrapperOperatingSystemMXBean(hsOs);
        }
        if (hsOs == null) return null;
        return new HotspotOSStatistics(os, hsOs, processors);
    }

    private AbstractStatistic createCpuStatistics() {
        java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        int processors = Math.max(1, os.getAvailableProcessors());
        OperatingSystemMXBean hsOs = null;
        if (os instanceof OperatingSystemMXBean) {
            hsOs = (OperatingSystemMXBean) os;
            if ("Linux".equals(OS_NAME)) hsOs = new LinuxMemInfoWrapperOperatingSystemMXBean(hsOs);
        }
        if (hsOs == null) return null;
        if (hsOs.getProcessCpuTime() > 0) return new CpuStatistics(hsOs, processors);
        return null;
    }

    protected void addProcessStatistics(StatisticCollector collector) {
        try {
            collector.register(new ProcessStatistics());
        } catch (Throwable ex) {
            collector.delayRegister(ProcessStatistics::new);
        }
    }

    protected void addSafepointStatistics(StatisticCollector collector) {
        try {
            collector.register(createSafepointStatistic());
        } catch (Throwable ex) {
            collector.delayRegister(this::createCpuStatistics);
        }
    }

    private AbstractStatistic createSafepointStatistic() {
        return new SafepointStatistics(ManagementFactoryHelper.getHotspotRuntimeMBean());
    }

    protected void addSynchronizationStatistics(StatisticCollector collector) {
        try {
            collector.register(createSyncronizationStatistics());
        } catch (Throwable ex) {
            collector.delayRegister(this::createSyncronizationStatistics);
        }
    }

    private AbstractStatistic createSyncronizationStatistics() {
        return new SynchronizationStatistics(ManagementFactoryHelper.getHotspotRuntimeMBean());
    }

    protected void addFileDescriptorStatistics(StatisticCollector collector) {
        try {
            AbstractStatistic fileDescriptorStatistics = createFileDescriptorStatistics();
            if (fileDescriptorStatistics != null) collector.register(fileDescriptorStatistics);
        } catch (Throwable ex) {
            collector.delayRegister(this::createFileDescriptorStatistics);
        }
    }

    private AbstractStatistic createFileDescriptorStatistics() {
        java.lang.management.OperatingSystemMXBean os = ManagementFactoryHelper.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean) {
            UnixOperatingSystemMXBean unixOs = (UnixOperatingSystemMXBean) os;
            return new FileDescriptorStatistics(unixOs);
        }
        return null;
    }

    /**
     * Wraps original OperatingSystemMXBean to correct Linux getFreePhysicalMemorySize. It does not take into account cache buffers (files cache).
     */
    static class LinuxMemInfoWrapperOperatingSystemMXBean implements OperatingSystemMXBean {
        private static final Pattern CACHED_PATTERN = Pattern.compile("Cached:\\s*(\\d+) kB$");

        private final OperatingSystemMXBean os;

        LinuxMemInfoWrapperOperatingSystemMXBean(OperatingSystemMXBean os) {
            this.os = os;
        }

        @Override
        public String getName() {
            return os.getName();
        }

        @Override
        public String getArch() {
            return os.getArch();
        }

        @Override
        public String getVersion() {
            return os.getVersion();
        }

        @Override
        public int getAvailableProcessors() {
            return os.getAvailableProcessors();
        }

        @Override
        public double getSystemLoadAverage() {
            return os.getSystemLoadAverage();
        }

        @Override
        public long getCommittedVirtualMemorySize() {
            return os.getCommittedVirtualMemorySize();
        }

        @Override
        public long getFreePhysicalMemorySize() {
            return os.getFreePhysicalMemorySize() + getLinuxFSCacheSize();
        }

        @Override
        public long getFreeSwapSpaceSize() {
            return os.getFreeSwapSpaceSize();
        }

        @Override
        public long getProcessCpuTime() {
            return os.getProcessCpuTime();
        }

        @Override
        public long getTotalPhysicalMemorySize() {
            return os.getTotalPhysicalMemorySize();
        }

        @Override
        public long getTotalSwapSpaceSize() {
            return os.getTotalSwapSpaceSize();
        }

        @Override
        public ObjectName getObjectName() {
            return null;
        }

        @Override
        public double getProcessCpuLoad() {
            return 0;
        }

        @Override
        public double getSystemCpuLoad() {
            return 0;
        }

        /**
         * @return current Linux FS cache buffers size. It reduces the free space so you may add it to free space to get actual free space.
         */
        static long getLinuxFSCacheSize() {
            File meminfo = new File("/proc/meminfo");
            if (!meminfo.exists() || !meminfo.canRead()) return 0;
            try {
                try (BufferedReader reader = new BufferedReader(new FileReader(meminfo))) {
                    return parseLinuxFSCacheSize(reader);
                }
            } catch (IOException ex) {
                return 0;
            }
        }

        static long parseLinuxFSCacheSize(BufferedReader reader) throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = CACHED_PATTERN.matcher(line);
                if (matcher.matches()) return Long.parseLong(matcher.group(1)) * 1024;
            }
            return 0;
        }
    }
}
