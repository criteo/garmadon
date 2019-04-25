package com.criteo.hadoop.garmadon.tool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Not designed to have no impact on the running app, but designed to be used
 * when debugging and it is not easy to launch a prod class profiler, maybe at immediate runtime,
 * throughout and agent...
 * It will produce files that can be consumed by FlameGraph (https://github.com/brendangregg/FlameGraph) to produce nice visualization
 */
public class TinySampler {
    private static final long SLEEP_PRECISION = TimeUnit.MILLISECONDS.toNanos(2);
    private final Thread sampledThread;
    private final int samplingIntervalMicro;
    private final Thread t = new Thread(new Sampler());
    private final HashSet<StackTraceElement[]> stacks = new HashSet<>();
    private final String name;
    private volatile boolean run = true;

    public TinySampler(String name, Thread sampledThread, int samplingIntervalMicro) {
        this.name = name;
        this.sampledThread = sampledThread;
        this.samplingIntervalMicro = samplingIntervalMicro;
    }

    public void start() {
        run = true;
        t.start();
        System.out.println("============ TINY SAMPLER " + name + " STARTED ============");
    }

    public Results stop() {
        run = false;
        try {
            t.join();
            System.out.println("============ TINY SAMPLER " + name + " STOPPED ============");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new Results(name, stacks);
    }

    private class Sampler implements Runnable {
        public void run() {
            while (run) {
                stacks.add(sampledThread.getStackTrace());
                sleepNanos(samplingIntervalMicro * 1000);
            }
        }

        /*
         * From http://andy-malakov.blogspot.fr/2010/06/alternative-to-threadsleep.html
         * No spinning implementation
         */
        private void sleepNanos(long nanoDuration) {
            final long end = System.nanoTime() + nanoDuration;
            long timeLeft = nanoDuration;
            do {
                if (timeLeft > SLEEP_PRECISION) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    Thread.yield();
                }
                timeLeft = end - System.nanoTime();
            } while (timeLeft > 0);
        }
    }

    public static class Results {
        private final String name;
        private final Collection<StackTraceElement[]> stacks;
        private final HashMap<String, Integer> unfolded;

        public Results(String name, Collection<StackTraceElement[]> stacks) {
            this.name = name;
            this.stacks = stacks;
            this.unfolded = new HashMap<>();
        }

        public void outputToTempFile() {
            try {
                outputToFile(File.createTempFile("easy-sampler-", ".samples"));
            } catch (IOException e) {
                System.err.println("TinySampler cannot output results. Reason: " + e.getMessage());
            }
        }

        public void outputToFile(File file) {
            try (FileOutputStream fos = new FileOutputStream(file)) {
                outputTo(fos);
                System.out.println("============ " + file.getAbsolutePath() + " ============");
            } catch (IOException e) {
                System.err.println("TinySampler cannot output results. Reason: " + e.getMessage());
            }
        }

        public void outputTo(OutputStream os) throws IOException {
            System.out.println("============ TINY SAMPLER RESULTS " + name + " ============");
            if (unfolded.isEmpty()) {
                unfoldStacks();
            }
            for (Map.Entry<String, Integer> e : unfolded.entrySet()) {
                String nameKey = e.getKey();
                int count = e.getValue();
                os.write((nameKey + " " + count + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }

        private void unfoldStacks() {
            for (StackTraceElement[] stack : stacks) {
                unfoldAndCountStack(stack);
            }
            stacks.clear(); //no need to maintain references when stacks are unfolded
        }

        private void unfoldAndCountStack(StackTraceElement[] els) {
            StringBuilder sb = new StringBuilder();
            for (int i = els.length - 1; i >= 0; i--) {
                StackTraceElement el = els[i];
                sb.append(el.getClassName());
                sb.append("`");
                sb.append(el.getMethodName());
                String part = sb.toString();
                Integer count = unfolded.get(part);
                if (count == null) {
                    count = 0;
                }
                unfolded.put(part, count + 1);
                sb.append(";");
            }
        }
    }
}