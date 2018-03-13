package com.criteo.hadoop.garmadon;

import com.criteo.hadoop.garmadon.benchmark.BenchmarkHelper;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * This benchmark wants to test the cost of retreiving and calling serialization/deserialization methods
 * without consideration of the deserialization itself
 * Since everything is kept in a registry that needs to be accessed for every event serialized/deserialized
 * it could be a bottleneck
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Fork(1)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class GarmadonSerializationRegistryBenchmark {

    private Random random = new Random();
    private Class[] eventClasses = new Class[1000];

    //objects that are reset at each iteration and used by benchmark methods
    private Class classToUse;
    private Object eventToUse;
    private int markerToUse;

    //inputstream that does nothing, all deserializer just create an instance
    private InputStream fakeInputStream = new InputStream() {
        @Override
        public int read() throws IOException {
            return 0;
        }
    };

    @Setup(Level.Trial)
    public void createEventClasses(){
        //create a classes to populate registry for the benchmark
        IntStream.range(0, 1000).forEach(this::createAndRegisterClass);
    }

    @Setup(Level.Invocation)
    public void setClassToUseForTrial() throws IllegalAccessException, InstantiationException {
        //for every invocation of benchmark methods, select a new event class
        markerToUse = random.nextInt(1000);
        classToUse = eventClasses[markerToUse];
        eventToUse = classToUse.newInstance();
    }

    //used for setup
    private void createAndRegisterClass(int index){
        Class aClass = BenchmarkHelper.createClassDefinition("Event"+index);
        //use very simple serializer and deserializers, our goal is to check how much it costs to access them
        GarmadonSerialization.register(aClass, index, o -> new byte[100], bytes -> {
            try {
                return aClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                return null;
            }
        });
        eventClasses[index] = aClass;
    }

    @Benchmark
    public int testGetMarker() throws TypeMarkerException {
        return GarmadonSerialization.getMarker(classToUse);
    }

    @Benchmark
    public byte[] testGetSerializer() throws SerializationException {
        return GarmadonSerialization.serialize(eventToUse);
    }

    @Benchmark
    public Object testGetDeserializer() throws DeserializationException {
        return GarmadonSerialization.parseFrom(markerToUse, fakeInputStream);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GarmadonSerializationRegistryBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
