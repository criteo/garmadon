package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NetworkStatistics extends AbstractStatistic {
    private static final String NETWORK_HEADER = "network";
    private static final String NETWORK_PROCESS_RECV = "processrx";
    private static final String NETWORK_PROCESS_SENT = "processtx";
    private static final Pattern SS_SOCKET_LINE_1 = Pattern.compile("ESTAB\\s+\\d+\\s+\\d+\\s+(\\d+\\.\\d+\\.\\d+.\\d+:\\d+)\\s+(\\d+\\.\\d+\\.\\d+.\\d+:\\d+)\\s+users:\\(\\(.*,pid=(\\d+),.*\\)\\)");
    private static final Pattern SS_SOCKET_LINE_RECEIVED = Pattern.compile("bytes_received:(\\d+)");
    private static final Pattern SS_SOCKET_LINE_ACKED = Pattern.compile("bytes_acked:(\\d+)");
    private static final File PROC_FILE = new File("/proc");

    private final int currentPid;
    private final Map<String, SocketStats> socketStats = new HashMap<>();

    public NetworkStatistics() {
        super(NETWORK_HEADER);
        this.currentPid = extractCurrentPid();
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        ProcessBuilder builder = new ProcessBuilder("/usr/sbin/ss", "-tinp");
        try {
            Process process = builder.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                return;
            }
            Map<Integer, ProcessNetworkStats> processNetworkStatsMap = getProcessNetworkStats(process);
            Map<Integer, ProcessInfo> processes = listProcess(processNetworkStatsMap);
            resolveProcessTree(processes);
            collectNetworkStats(processes, sink);
        } catch (IOException ignored) {

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void collectNetworkStats(Map<Integer, ProcessInfo> processes, StatisticsSink sink) {
        ProcessInfo processInfo = processes.get(currentPid);
        updateSocketStats(processInfo);
        for (ProcessInfo child : processInfo.children) {
            updateSocketStats(child);
        }
        long recvBytes = 0;
        long sentBytes = 0;
        for (SocketStats ss : socketStats.values()) {
            recvBytes += ss.bytesReceived;
            sentBytes += ss.bytesAcked;
        }
        sink.add(NETWORK_PROCESS_RECV, recvBytes);
        sink.add(NETWORK_PROCESS_SENT, sentBytes);
    }

    private void updateSocketStats(ProcessInfo processInfo) {
        if (processInfo.netStats != null) {
            socketStats.putAll(processInfo.netStats.socketStats);
        }
    }

    private Map<Integer, ProcessNetworkStats> getProcessNetworkStats(Process process) throws IOException {
        Map<Integer, ProcessNetworkStats> processNetworkStatsMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            int pid = -1;
            String localSocket = null;
            String remoteSocket = null;
            while ((line = reader.readLine()) != null) {
                if (pid == -1) {
                    Matcher line1 = SS_SOCKET_LINE_1.matcher(line);
                    if (line1.find()) {
                        localSocket = line1.group(1);
                        remoteSocket = line1.group(2);
                        pid = Integer.valueOf(line1.group(3));
                    }
                } else {
                    long bytesAcked = 0;
                    Matcher acked = SS_SOCKET_LINE_ACKED.matcher(line);
                    if (acked.find()) {
                        bytesAcked = Long.valueOf(acked.group(1));
                    }
                    long bytesReceived = 0;
                    Matcher received = SS_SOCKET_LINE_RECEIVED.matcher(line);
                    if (received.find()) {
                        bytesReceived = Long.valueOf(received.group(1));
                    }
                    ProcessNetworkStats processNetworkStats = processNetworkStatsMap.computeIfAbsent(pid, ProcessNetworkStats::new);
                    processNetworkStats.put(localSocket + "#" + remoteSocket, bytesAcked, bytesReceived);
                    pid = -1;
                }
            }
        }
        return processNetworkStatsMap;
    }

    private static boolean isAllDigits(File dir, String name) {
        for (int i = 0; i < name.length(); i++) {
            if (!Character.isDigit(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Map<Integer, ProcessInfo> listProcess(Map<Integer, ProcessNetworkStats> processNetworkStatsMap) {
        String[] pids = PROC_FILE.list(NetworkStatistics::isAllDigits);
        if (pids == null) {
            return Collections.emptyMap();
        }
        Map<Integer, ProcessInfo> processes = new HashMap<>();
        for (String pid : pids) {
            try {
                try (RandomAccessFile raf = new RandomAccessFile("/proc/" + pid + "/stat", "r")) {
                    String[] stat = LinuxHelper.getFileLineSplit(raf);
                    if (stat.length < 24) {
                        continue;
                    }
                    int intPid = Integer.valueOf(pid);
                    int parentPid = Integer.valueOf(stat[3]);
                    ProcessInfo pi = processes.computeIfAbsent(intPid, intpid -> new ProcessInfo(intPid, parentPid));
                    ProcessNetworkStats processNetworkStats = processNetworkStatsMap.get(intPid);
                    if (processNetworkStats != null) {
                        pi.netStats = processNetworkStats;
                    }
                }
            } catch (IOException ignored) {
            }
        }
        return processes;
    }

    private void resolveProcessTree(Map<Integer, ProcessInfo> processes) {
        for (ProcessInfo pi : processes.values()) {
            ProcessInfo parent = processes.get(pi.parentPid);
            if (parent != null) {
                parent.addChild(pi);
            }
        }
    }

    private int extractCurrentPid() {
        String vmName = ManagementFactory.getRuntimeMXBean().getName();
        int idx = vmName.indexOf('@');
        if (idx == -1) {
            return -1;
        }
        return Integer.valueOf(vmName.substring(0, idx));
    }

    private static class ProcessInfo {
        private final int pid;
        private final int parentPid;
        private List<ProcessInfo> children = Collections.emptyList();
        private ProcessNetworkStats netStats;

        public ProcessInfo(int pid, int parentPid) {
            this.pid = pid;
            this.parentPid = parentPid;
        }

        public void addChild(ProcessInfo child) {
            if (children == Collections.<ProcessInfo>emptyList()) {
                children = new ArrayList<>();
            }
            children.add(child);
        }
    }

    private static class ProcessNetworkStats {
        private final int pid;
        private Map<String, SocketStats> socketStats = new HashMap<>();

        public ProcessNetworkStats(int pid) {
            this.pid = pid;
        }

        public void put(String key, long bytesAcked, long bytesReceived) {
            SocketStats socketStats = this.socketStats.computeIfAbsent(key, k -> new SocketStats(k, bytesAcked, bytesReceived));
            socketStats.update(bytesAcked, bytesReceived);
        }
    }

    private static class SocketStats {
        private final String key;
        private long bytesAcked;
        private long bytesReceived;

        public SocketStats(String key, long bytesAcked, long bytesReceived) {
            this.key = key;
            this.bytesAcked = bytesAcked;
            this.bytesReceived = bytesReceived;
        }

        public void update(long bytesAcked, long bytesReceived) {
            this.bytesAcked = bytesAcked;
            this.bytesReceived = bytesReceived;
        }
    }
}
