package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GarmadonSparkStorageStatusListenerIntegrationTest {

    private TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;

    private JavaSparkContext jsc;
    private SparkContext sc;
    private GarmadonSparkStorageStatusListener sparkListener;

    private static class RDDInfo {
        private long offHeap;
        private long memory;
        private long disk;
    }

    private static class ExecutorInfo {
        private long rddOffHeap;
        private long rddMemory;
        private long rddDisk;
        private long broadcastOffHeap;
        private long broadcastMemory;
        private long broadcastDisk;
    }

    private HashMap<String, RDDInfo> rdds;
    private HashMap<String, ExecutorInfo> executors;

    @Before
    public void setUp() {
        rdds = new HashMap<>();
        executors = new HashMap<>();

        header = Header.newBuilder()
            .withId("id")
            .addTag(Header.Tag.STANDALONE.name())
            .withHostname("host")
            .withUser("user")
            .withPid("pid")
            .buildSerializedHeader();

        eventHandler = (aLong, header, o) -> {
            if (o instanceof SparkEventProtos.ExecutorStorageStatus) {
                SparkEventProtos.ExecutorStorageStatus event = (SparkEventProtos.ExecutorStorageStatus) o;
                ExecutorInfo ei = new ExecutorInfo();
                ei.broadcastDisk = event.getBroadcastDiskUsed();
                ei.broadcastMemory = event.getBroadcastMemoryUsed();
                ei.broadcastOffHeap = event.getBroadcastOffHeapMemoryUsed();
                ei.rddDisk = event.getRddDiskUsed();
                ei.rddMemory = event.getRddMemoryUsed();
                ei.rddOffHeap = event.getRddOffHeapMemoryUsed();
                executors.put(header.getExecutorId(), ei);
            }
            if (o instanceof SparkEventProtos.RDDStorageStatus) {
                SparkEventProtos.RDDStorageStatus event = (SparkEventProtos.RDDStorageStatus) o;
                RDDInfo ri = new RDDInfo();
                ri.disk = event.getDiskUsed();
                ri.memory = event.getMemoryUsed();
                ri.offHeap = event.getOffHeapMemoryUsed();
                rdds.put(event.getRddName(), ri);
            }
        };

        SparkListernerConf.getInstance().setConsumer(eventHandler);
        SparkListernerConf.getInstance().setHeader(header);

        jsc = new JavaSparkContext(
            new SparkConf()
                .setAppName("TestGarmadonListener")
                .setMaster("local[4]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.memory.offHeap.enabled", "true")
                .set("spark.memory.offHeap.size", "50m")
        );
        sc = jsc.sc();

        sparkListener = new GarmadonSparkStorageStatusListener();
        sc.addSparkListener(sparkListener);
    }

    @After
    public void tearDown() {
        jsc.close();
    }

    @Test
    public void SparkStorageStatusListener_should_track_rdd_storage_status() throws InterruptedException {
        assertTrue(rdds.isEmpty());
        assertTrue(executors.isEmpty());

        //Memory
        JavaRDD rddMemory = makeRDD("MemRDD", StorageLevel.MEMORY_ONLY());
        rddMemory.collect();

        checkRddStorage(rddMemory.name(), equalTo(0L), greaterThan(0L), equalTo(0L));

        //Disk
        JavaRDD rddDisk = makeRDD("DiskRDD", StorageLevel.DISK_ONLY());
        rddDisk.collect();

        checkRddStorage(rddDisk.name(), equalTo(0L), equalTo(0L), greaterThan(0L));

        //OffHeap
        JavaRDD rddOffHeap = makeRDD("OffHeapRDD", StorageLevel.OFF_HEAP());
        rddOffHeap.collect();

        checkRddStorage(rddOffHeap.name(), greaterThan(0L), equalTo(0L), equalTo(0L));
    }

    @Test
    public void SparkStorageStatusListener_should_track_executor_storage_status() throws InterruptedException {
        assertTrue(rdds.isEmpty());
        assertTrue(executors.isEmpty());

        //Memory
        JavaRDD rddMemory = makeRDD("MemRDD", StorageLevel.MEMORY_ONLY());
        rddMemory.collect();

        checkExecutorRDDStorage("driver", equalTo(0L), greaterThan(0L), equalTo(0L));

        //Disk
        JavaRDD rddDisk = makeRDD("DiskRDD", StorageLevel.DISK_ONLY());
        rddDisk.collect();

        checkExecutorRDDStorage("driver", equalTo(0L), greaterThan(0L), greaterThan(0L));

        //OffHeap
        JavaRDD rddOffHeap = makeRDD("OffHeapRDD", StorageLevel.OFF_HEAP());
        rddOffHeap.collect();

        checkExecutorRDDStorage("driver", greaterThan(0L), greaterThan(0L), greaterThan(0L));

        rddMemory.unpersist(true);
        //wait for the EventBus to fire the unpersistRDD event
        Thread.sleep(1000);
        checkExecutorRDDStorage("driver", greaterThan(0L), equalTo(0L), greaterThan(0L));

        rddDisk.unpersist(true);
        //wait for the EventBus to fire the unpersistRDD event
        Thread.sleep(1000);
        checkExecutorRDDStorage("driver", greaterThan(0L), equalTo(0L), equalTo(0L));

        rddOffHeap.unpersist(true);
        //wait for the EventBus to fire the unpersistRDD event
        Thread.sleep(1000);
        checkExecutorRDDStorage("driver", equalTo(0L), equalTo(0L), equalTo(0L));
    }

    @Test
    public void SparkStorageStatusListener_should_track_broadcast_storage_status() throws InterruptedException {
        assertTrue(rdds.isEmpty());
        assertTrue(executors.isEmpty());

        Broadcast<List<UUID>> broadcast = jsc.broadcast(IntStream.range(0, 1000000).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList()));

        jsc.parallelize(Collections.singletonList(1)).map(i -> broadcast.getValue()).collect();

        checkExecutorBroadcastStorage("driver", equalTo(0L), greaterThan(0L), equalTo(0L));
    }

    private void checkRddStorage(String name, Matcher<Long> offHeapMatcher, Matcher<Long> memoryMatcher, Matcher<Long> diskMatcher) {
        RDDInfo info = rdds.get(name);
        assertThat("offheap storage has unexpected value", info.offHeap, offHeapMatcher);
        assertThat("memory storage has unexpected value", info.memory, memoryMatcher);
        assertThat("disk storage has unexpected value", info.disk, diskMatcher);
    }

    private void checkExecutorRDDStorage(String name, Matcher<Long> offHeapMatcher, Matcher<Long> memoryMatcher, Matcher<Long> diskMatcher) {
        ExecutorInfo info = executors.get(name);
        assertThat("rdd offheap storage has unexpected value", info.rddOffHeap, offHeapMatcher);
        assertThat("rdd memory storage has unexpected value", info.rddMemory, memoryMatcher);
        assertThat("rdd disk storage has unexpected value", info.rddDisk, diskMatcher);
    }

    private void checkExecutorBroadcastStorage(String name, Matcher<Long> offHeapMatcher, Matcher<Long> memoryMatcher, Matcher<Long> diskMatcher) {
        ExecutorInfo info = executors.get(name);
        assertThat("broadcast offheap storage has unexpected value", info.broadcastOffHeap, offHeapMatcher);
        assertThat("broadcast memory storage has unexpected value", info.broadcastMemory, memoryMatcher);
        assertThat("broadcast disk storage has unexpected value", info.broadcastDisk, diskMatcher);
    }


    private JavaRDD makeRDD(String name, StorageLevel storageLevel) {
        return jsc.parallelize(IntStream.range(0, 1000000).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList()))
            .persist(storageLevel)
            .setName(name);
    }
}
