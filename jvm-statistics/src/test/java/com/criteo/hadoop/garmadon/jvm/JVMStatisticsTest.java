package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.jvm.utils.JavaRuntime;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class JVMStatisticsTest {
    private static final Pattern JVM_8_STATS_PATTERN = Pattern.compile(
            "gc\\(.*\\)\\[count=\\d+, time=\\d+], gc\\(.*\\)\\[count=\\d+, time=\\d+], " +
            "heap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "nonheap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "code\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "metaspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "compressedclassspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "eden\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "survivor\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "old\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "compile\\[count=\\d+, time=\\d+, invalidated=\\d+, failed=\\d+, threads=\\d+], " +
            "threads\\[count=\\d+, daemon=\\d+, total=\\d+, internal=\\d+], " +
            "class\\[loaded=\\d+, unloaded=\\d+, initialized=\\d+, loadtime=\\d+, inittime=\\d+, veriftime=\\d+], " +
            "os\\[%loadavg=\\d+\\.\\d+, physicalfree=\\d+, physicaltotal=\\d+, swapfree=\\d+, swaptotal=\\d+, virtual=\\d+], " +
            "cpu\\[cores=\\d+, %load=\\d+\\.\\d+], " +
            "process\\[threads=\\d+, rss=\\d+, vsz=\\d+, %user=\\d+\\.\\d+, %sys=\\d+\\.\\d+, read=\\d+, written=\\d+.*], " +
            "safepoints\\[count=\\d+, synctime=\\d+, totaltime=\\d+], " +
            "synclocks\\[contendedlockattempts=\\d+, deflations=\\d+, futilewakeups=\\d+, inflations=\\d+, monextant=\\d+, notifications=\\d+, parks=\\d+].*"
    );

    /**
     * Example: <pre>
     * gc(G1 Young Generation)[count=1, time=4], gc(G1 Old Generation)[count=0, time=0],
     * heap[used=57512, committed=524288, init=524288, max=524288],
     * nonheap[used=24389, committed=28480, init=7488, max=0],
     * CodeHeap 'non-nmethods'[used=1234, committed=2496, init=2496, max=5700], metaspace[used=17129, committed=17792, init=0, max=0],
     * metaspace[used=17129, committed=17792, init=0, max=0]
     * CodeHeap 'profiled nmethods'[used=3748, committed=3776, init=2496, max=120028],
     * compressedclassspace[used=1749, committed=1920, init=0, max=1048576],
     * eden[used=53248, committed=326656, init=27648, max=0],
     * old[used=168, committed=193536, init=496640, max=524288],
     * survivor[used=4096, committed=4096, init=0, max=0],
     * CodeHeap 'non-profiled nmethods'[used=526, committed=2496, init=2496, max=120032],
     * compile[count=1554, time=1637, invalidated=0, failed=0, threads=4],
     * threads[count=9, daemon=5, total=9, internal=20],
     * class[loaded=2668, unloaded=0, initialized=2115, loadtime=1135, inittime=546, veriftime=293],
     * os[%loadavg=28.0, physicalfree=57976, physicaltotal=33554432, swapfree=780288, swaptotal=6291456, virtual=37905728],
     * cpu[cores=12, %load=14.524648],
     * process[threads=36, rss=173312, vsz=37905728, %user=22.0, %sys=2.0, read=0, written=0],
     * safepoints[count=146, synctime=1, totaltime=17],
     * synclocks[contendedlockattempts=2, deflations=303, futilewakeups=0, inflations=305, monextant=2, notifications=3, parks=5]
     * </pre>
     */
    private static final Pattern JVM_9_STATS_PATTERN = Pattern.compile(
            "gc\\(.*\\)\\[count=\\d+, time=\\d+], gc\\(.*\\)\\[count=\\d+, time=\\d+], " +
            "heap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "nonheap\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "CodeHeap 'non-nmethods'\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "metaspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "CodeHeap 'profiled nmethods'\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "compressedclassspace\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "eden\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "old\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "survivor\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "CodeHeap 'non-profiled nmethods'\\[used=\\d+, committed=\\d+, init=\\d+, max=\\d+], " +
            "compile\\[count=\\d+, time=\\d+, invalidated=\\d+, failed=\\d+, threads=\\d+], " +
            "threads\\[count=\\d+, daemon=\\d+, total=\\d+, internal=\\d+], " +
            "class\\[loaded=\\d+, unloaded=\\d+, initialized=\\d+, loadtime=\\d+, inittime=\\d+, veriftime=\\d+], " +
            "os\\[%loadavg=\\d+\\.\\d+, physicalfree=\\d+, physicaltotal=\\d+, swapfree=\\d+, swaptotal=\\d+, virtual=\\d+], " +
            "cpu\\[cores=\\d+, %load=\\d+\\.\\d+], " +
            "process\\[threads=\\d+, rss=\\d+, vsz=\\d+, %user=\\d+\\.\\d+, %sys=\\d+\\.\\d+, read=\\d+, written=\\d+.*], " +
            "safepoints\\[count=\\d+, synctime=\\d+, totaltime=\\d+], " +
            "synclocks\\[contendedlockattempts=\\d+, deflations=\\d+, futilewakeups=\\d+, inflations=\\d+, monextant=\\d+, notifications=\\d+, parks=\\d+].*"
    );
    private static final Pattern GCSTATS_PATTERN = Pattern.compile(".* occurred at \\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+, took \\d+ms \\(System\\.gc\\(\\)\\) {2}(eden|survivor|old)\\[[-+]\\d+]\\(\\d+->\\d+\\) (eden|survivor|old)\\[[-+]\\d+]\\(\\d+->\\d+\\).*");
    private static final Pattern MACHINESTATS_PATTERN = Pattern.compile("machinecpu\\[%user=\\d+\\.\\d+, %nice=\\d+\\.\\d+, %sys=\\d+\\.\\d+, %idle=\\d+\\.\\d+, %iowait=\\d+\\.\\d+, %irq=\\d+\\.\\d+, %softirq=\\d+\\.\\d+, %core0=\\d+\\.\\d+.*], memory\\[swap=\\d+, physical=\\d+], network\\[.*_rx=\\d+, .*_tx=\\d+, .*_pktrx=\\d+, .*_pkttx=\\d+, .*_errin=\\d+, .*_errout=\\d+.*], disk\\[.*_reads=\\d+, .*_readbytes=\\d+, .*_writes=\\d+, .*_writebytes=\\d+.*]");

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicBoolean isMatches = new AtomicBoolean();
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
        Pattern pattern = JavaRuntime.getVersion() == 8 ? JVM_8_STATS_PATTERN : JVM_9_STATS_PATTERN;
        isMatches.set(pattern.matcher(s).matches());
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
