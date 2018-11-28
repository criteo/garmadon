package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.util.ExecutingCommand;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;

class ProcessStatistics extends AbstractStatistic {
    private static final String PROCESS_HEADER = "process";
    private static final String PROCESS_VAR_THREADS = "threads";
    private static final String PROCESS_VAR_RSS = "rss";
    private static final String PROCESS_VAR_VSZ = "vsz";
    private static final String PROCESS_VAR_USER = "user";
    private static final String PROCESS_VAR_SYS = "sys";
    private static final String PROCESS_VAR_READ = "read";
    private static final String PROCESS_VAR_WRITTEN = "written";
    private static final String PROCESS_VAR_CTXTSWITCHES = "ctxtswitches";
    private static final String PROCESS_VAR_INTERRUPTS = "interrupts";

    private OperatingSystem os;
    private final int currentPid;
    private long prevUser;
    private long prevSys;
    private long prevTimeStamp;
    private final RandomAccessFile procStat;
    private final RandomAccessFile procStatus;
    private final RandomAccessFile procIO;
    private final long pageSize;
    private final long hz;

    ProcessStatistics() {
        super(PROCESS_HEADER);
        prevTimeStamp = System.currentTimeMillis();
        procStat = LinuxHelper.openFile(new File("/proc/self/stat"));
        procStatus = LinuxHelper.openFile(new File("/proc/self/status"));
        procIO = LinuxHelper.openFile(new File("/proc/self/io"));
        if (procStat == null) {
            this.os = new SystemInfo().getOperatingSystem();
            currentPid = os.getProcessId();
            OSProcess process = os.getProcess(currentPid);
            prevUser = process.getUserTime();
            prevSys = process.getKernelTime();
            pageSize = 0;
            hz = 0;
        } else {
            currentPid = 0;
            String[] split = LinuxHelper.getFileLineSplit(procStat);
            if (split.length >= 24) {
                prevUser = Long.parseLong(split[13]);
                prevSys = Long.parseLong(split[14]);
            }
            pageSize = LinuxHelper.getPageSize();
            hz = getClockTick();
        }
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        long currentTimeStamp = System.currentTimeMillis();
        if (procStat == null) {
            OSProcess process = os.getProcess(currentPid); // Used for Windows
            sink.add(PROCESS_VAR_THREADS, process.getThreadCount());
            sink.add(PROCESS_VAR_RSS, process.getResidentSetSize() / 1024);
            sink.add(PROCESS_VAR_VSZ, process.getVirtualSize() / 1024);
            long currentUser = process.getUserTime();
            long currentSys = process.getKernelTime();
            sink.addPercentage(PROCESS_VAR_USER, roundPercentage(currentUser - prevUser, currentTimeStamp - prevTimeStamp));
            sink.addPercentage(PROCESS_VAR_SYS, roundPercentage(currentSys - prevSys, currentTimeStamp - prevTimeStamp));
            prevUser = currentUser;
            prevSys = currentSys;
            sink.addSize(PROCESS_VAR_READ, process.getBytesRead());
            sink.addSize(PROCESS_VAR_WRITTEN, process.getBytesWritten());
            return;
        }
        String[] split = LinuxHelper.getFileLineSplit(procStat);
        if (split.length < 24) return;
        sink.add(PROCESS_VAR_THREADS, split[19]);
        sink.add(PROCESS_VAR_RSS, Long.parseLong(split[23]) * pageSize / 1024);
        sink.add(PROCESS_VAR_VSZ, Long.parseLong(split[22]) / 1024);
        long currentUser = Long.parseLong(split[13]);
        long currentSys = Long.parseLong(split[14]);
        sink.addPercentage(PROCESS_VAR_USER, roundPercentage((currentUser - prevUser) * 1000 / hz, currentTimeStamp - prevTimeStamp));
        sink.addPercentage(PROCESS_VAR_SYS, roundPercentage((currentSys - prevSys) * 1000 / hz, currentTimeStamp - prevTimeStamp));
        prevUser = currentUser;
        prevSys = currentSys;
        prevTimeStamp = currentTimeStamp;
        collectIO(procIO, sink);
        collectStatus(procStatus, sink);
    }

    @Override
    public void close() {
        RandomAccessFile raf = procStatus;
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
            }
        }
        raf = procStat;
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
            }
        }
        raf = procIO;
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
            }
        }
    }

    static void collectIO(RandomAccessFile procIO, StatisticsSink sink) {
        if (procIO == null) return;
        try {
            procIO.seek(0);
            String line;
            while ((line = procIO.readLine()) != null) {
                if (line.startsWith("read_bytes:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.addSize(PROCESS_VAR_READ, Long.parseLong(matcher.group(1)));
                    continue;
                }
                if (line.startsWith("write_bytes:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.addSize(PROCESS_VAR_WRITTEN, Long.parseLong(matcher.group(1)));
                }
            }
        } catch (IOException ignored) {
        }
    }

    static void collectStatus(RandomAccessFile procStatus, StatisticsSink sink) {
        if (procStatus == null) return;
        try {
            procStatus.seek(0);
            String line;
            while ((line = procStatus.readLine()) != null) {
                if (line.startsWith("read_bytes:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.addSize(PROCESS_VAR_READ, Long.parseLong(matcher.group(1)));
                    continue;
                }
                if (line.startsWith("write_bytes:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.addSize(PROCESS_VAR_WRITTEN, Long.parseLong(matcher.group(1)));
                    continue;
                }
                if (line.startsWith("voluntary_ctxt_switches:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.add(PROCESS_VAR_CTXTSWITCHES, matcher.group(1));
                    continue;
                }
                if (line.startsWith("nonvoluntary_ctxt_switches:")) {
                    Matcher matcher = LinuxHelper.COUNT_PATTERN.matcher(line);
                    if (matcher.find()) sink.add(PROCESS_VAR_INTERRUPTS, matcher.group(1));
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static long getClockTick() {
        long res = Long.parseLong(ExecutingCommand.runNative(new String[]{"getconf", "CLK_TCK"}).get(0));
        if (res == 0) res = 100;
        return res;
    }

    private static int roundPercentage(long part, long total) {
        double d = 100 * part / (double) total;
        return (int) Math.round(d);
    }
}
