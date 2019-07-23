package com.criteo.hadoop.garmadon.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class JVMStatisticsTest {
    private static final Pattern JVMSTATS_PATTERN = Pattern.compile("gc\\(.*\\)\\[count=\\d+, time=\\d+\\], gc\\(.*\\)\\[count=\\d+, time=\\d+\\], " +
            "heap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "nonheap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "code\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "metaspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "compressedclassspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "eden\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "survivor\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "old\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+\\], " +
            "compile\\[count=\\d+, time=\\d+, invalidated=\\d+, failed=\\d+, threads=\\d+\\], " +
            "threads\\[count=\\d+, daemon=\\d+, total=\\d+, internal=\\d+\\], " +
            "class\\[loaded=\\d+, unloaded=\\d+, initialized=\\d+, loadtime=\\d+, inittime=\\d+, veriftime=\\d+\\], " +
            "os\\[%loadavg=\\d+\\.\\d+, physicalfree=\\d+, physicaltotal=\\d+, swapfree=\\d+, swaptotal=\\d+, virtual=\\d+\\], " +
            "cpu\\[cores=\\d+, %load=\\d+\\.\\d+\\], " +
            "process\\[threads=\\d+, rss=\\d+, vsz=\\d+, %user=\\d+\\.\\d+, %sys=\\d+\\.\\d+, read=\\d+, written=\\d+.*\\], " +
            "safepoints\\[count=\\d+, synctime=\\d+, totaltime=\\d+\\], " +
            "synclocks\\[contendedlockattempts=\\d+, deflations=\\d+, futilewakeups=\\d+, inflations=\\d+, monextant=\\d+, notifications=\\d+, parks=\\d+\\].*");
    private static final Pattern GCSTATS_PATTERN = Pattern.compile(".* occurred at \\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+, took \\d+ms \\(System\\.gc\\(\\)\\)  (eden|survivor|old)\\[[-+]\\d+\\]\\(\\d+->\\d+\\) (eden|survivor|old)\\[[-+]\\d+\\]\\(\\d+->\\d+\\).*");
    private static final Pattern MACHINESTATS_PATTERN = Pattern.compile("machinecpu\\[%user=\\d+\\.\\d+, %nice=\\d+\\.\\d+, %sys=\\d+\\.\\d+, %idle=\\d+\\.\\d+, %iowait=\\d+\\.\\d+, %irq=\\d+\\.\\d+, %softirq=\\d+\\.\\d+, %core0=\\d+\\.\\d+.*\\], memory\\[swap=\\d+, physical=\\d+\\], network\\[.*_rx=\\d+, .*_tx=\\d+, .*_pktrx=\\d+, .*_pkttx=\\d+, .*_errin=\\d+, .*_errout=\\d+.*\\], disk\\[.*_reads=\\d+, .*_readbytes=\\d+, .*_writes=\\d+, .*_writebytes=\\d+.*\\]");

    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicBoolean isMatches = new AtomicBoolean();
    private volatile String logLine;

    @Test
    public void startJvmStatsOnly() throws InterruptedException {
        Conf<String, String, String> conf = new Conf<>();
        conf.setInterval(Duration.ofSeconds(1));
        conf.setLogJVMStats(this::assertJVMStatsLog);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
        Assert.assertTrue("timeout", latch.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(logLine, isMatches.get());
        jvmStatistics.stop();
    }

    @Test
    public void startGCStatsOnly() throws InterruptedException {
        Conf<String, String, String> conf = new Conf<>();
        conf.setInterval(Duration.ofSeconds(1));
        conf.setLogGcStats(this::assertGCStatsLog);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
        System.gc();
        Assert.assertTrue("timeout", latch.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(logLine, isMatches.get());
        jvmStatistics.stop();
    }

    @Test
    public void startMachineStatsOnly() throws InterruptedException {
        Conf<String, String, String> conf = new Conf<>();
        conf.setInterval(Duration.ofSeconds(1));
        conf.setLogMachineStats(this::assertMachineStatsLog);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        jvmStatistics.start();
        Assert.assertTrue("timeout", latch.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(logLine, isMatches.get());
        jvmStatistics.stop();
    }

    @Test
    public void delayStart() throws InterruptedException {
        Conf<String, String, String> conf = new Conf<>();
        conf.setInterval(Duration.ofSeconds(1));
        conf.setLogJVMStats(this::assertJVMStatsLog);
        JVMStatistics jvmStatistics = new JVMStatistics(conf);
        long start = System.currentTimeMillis();
        jvmStatistics.delayStart(1);
        Assert.assertTrue("timeout", latch.await(10, TimeUnit.SECONDS));
        long stop = System.currentTimeMillis();
        long delay = stop - start;
        Assert.assertTrue(String.format("delay: %d", delay),delay >= TimeUnit.SECONDS.toMillis(1));
        Assert.assertTrue(isMatches.get());
    }

    private void assertJVMStatsLog(Long timestamp, String s) {
        logLine = s;
        isMatches.set(JVMSTATS_PATTERN.matcher(s).matches());
        latch.countDown();
    }

    private void assertGCStatsLog(Long timestamp, String s) {
        logLine = s;
        isMatches.set(GCSTATS_PATTERN.matcher(s).matches());
        latch.countDown();
    }

    private void assertMachineStatsLog(Long timestamp, String s) {
        logLine = s;
        isMatches.set(MACHINESTATS_PATTERN.matcher(s).matches());
        latch.countDown();
    }
}