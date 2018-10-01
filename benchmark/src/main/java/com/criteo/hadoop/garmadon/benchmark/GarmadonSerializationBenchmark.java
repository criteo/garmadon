package com.criteo.hadoop.garmadon.benchmark;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark class to measure serialization of events produced by garmadon
 * Queue implementation needs to follow the one used in garmadon agent
 * We test here the max number of event that can be produced. The appender used for benchmarking is very fast
 * In practice, the SocketAppender could apply backpressure.
 * Since we want to measure the serialization cost and queue implementation altogether it is fine to use
 * such a fast appender.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(3)
@Fork(1)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class GarmadonSerializationBenchmark {

    @Param({"10", "100", "1000"})
    int queueSize;

    BlockingQueue<Object> queue;
    AsyncEventProcessor asyncEventProcessor;

    @Setup
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        queue = new ArrayBlockingQueue<>(queueSize);
        asyncEventProcessor = new AsyncEventProcessor(bytes -> Blackhole.consumeCPU(10));//consume some CPU to prevent any optim
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void baseline(){ //determine the cost of Blackhole.consumeCPU(1)
        Blackhole.consumeCPU(10);
    }

    @Benchmark
    public Object test_produce_FsEvent() throws InterruptedException {
        FsEvent fsEvent = new FsEvent(System.currentTimeMillis(), "/tmp/testpatch/teragen/_temporary/1/task_1517228004270_0053_m_000014", FsEvent.Action.READ, "hdfs://root");
        queue.put(fsEvent); //use put to block teh thread and thus really see the max number of events we can consume
        return fsEvent;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GarmadonSerializationBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
