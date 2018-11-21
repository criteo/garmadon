package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EventHelperTest {
    HashMap<String, Map<String, Object>> eventMaps;
    Long timestamp = 1530175138000L;

    @Before
    public void setUp() {
        eventMaps = new HashMap<>();
    }


    @Test
    public void initEvent_should_return_a_hashmap() {
        String type = "TEST";
        Map<String, Object> res = EventHelper.initEvent(type);
        Assert.assertEquals(type, res.get("event_type"));
    }

    @Test
    public void processPathEvent_should_add_a_hashmap_with_path() {
        String path = "datadisco:/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.PathEvent event = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setType("INPUT")
                .setPath(path)
                .build();
        EventHelper.processPathEvent(GarmadonSerialization.getTypeName(0), event, eventMaps);
        Assert.assertEquals(path, eventMaps.get(GarmadonSerialization.getTypeName(0)).get("path"));
    }

    @Test
    public void processFsEvent_should_add_a_hashmap_with_dst_path() {
        String path = "/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent
                .newBuilder()
                .setAction("RENAME")
                .setDstPath(path)
                .setUri("hdfs://root")
                .build();
        EventHelper.processFsEvent(GarmadonSerialization.getTypeName(1), event, eventMaps);
        Assert.assertEquals(path, eventMaps.get(GarmadonSerialization.getTypeName(1)).get("dst_path"));
    }

    @Test
    public void processFsEvent_remove_uri_from_dst_payj() {
        String uri = "hdfs://root";
        String path = uri + "/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent
                .newBuilder()
                .setAction("RENAME")
                .setDstPath(path)
                .setUri(uri)
                .build();
        EventHelper.processFsEvent(GarmadonSerialization.getTypeName(1), event, eventMaps);
        Assert.assertEquals(path.replace(uri, ""), eventMaps.get(GarmadonSerialization.getTypeName(1)).get("dst_path"));
    }

    @Test
    public void processStateEvent_should_add_a_hashmap_with_state() {
        String state = "END";
        DataAccessEventProtos.StateEvent event = DataAccessEventProtos.StateEvent
                .newBuilder()
                .setState(state)
                .build();
        EventHelper.processStateEvent(GarmadonSerialization.getTypeName(3), event, eventMaps);
        Assert.assertEquals(state, eventMaps.get(GarmadonSerialization.getTypeName(3)).get("state"));
    }


    @Test
    public void processJVMStatisticsData_should_add_3_hashmap_disk_network_and_jvm() {
        // Disk section
        String disk = "sda";
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyDisk = JVMStatisticsEventsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName(disk + "_write")
                .setValue("10000")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionDisk = JVMStatisticsEventsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("disk")
                .addProperty(propertyDisk)
                .build();

        // Network Section
        String network = "enp129s0";
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyNetwork = JVMStatisticsEventsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName(network + "_rx")
                .setValue("100000")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionNetwork = JVMStatisticsEventsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("network")
                .addProperty(propertyNetwork)
                .build();


        // Others Section
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyOther = JVMStatisticsEventsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName("physical")
                .setValue("499")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionOther1 = JVMStatisticsEventsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("memory")
                .addProperty(propertyOther)
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionOther2 = JVMStatisticsEventsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("machinecpu")
                .addProperty(propertyOther)
                .build();

        JVMStatisticsEventsProtos.JVMStatisticsData event = JVMStatisticsEventsProtos.JVMStatisticsData
                .newBuilder()
                .addSection(sectionDisk)
                .addSection(sectionNetwork)
                .addSection(sectionOther1)
                .addSection(sectionOther2)
                .build();
        EventHelper.processJVMStatisticsData(GarmadonSerialization.getTypeName(1001), event, eventMaps);
        Assert.assertEquals(disk, eventMaps.get(disk).get("disk"));
        Assert.assertEquals(network, eventMaps.get(network).get("network"));
        Assert.assertEquals(499.0, eventMaps.get(GarmadonSerialization.getTypeName(1001)).get("memory_physical"));
        Assert.assertEquals(499.0, eventMaps.get(GarmadonSerialization.getTypeName(1001)).get("machinecpu_physical"));
    }


    @Test
    public void processGCStatisticsData_should_add_a_hashmap_with_path() {
        String collector_name = "PS Scavenge";
        JVMStatisticsEventsProtos.GCStatisticsData event = JVMStatisticsEventsProtos.GCStatisticsData
                .newBuilder()
                .setCause("Allocation Failure")
                .setCollectorName(collector_name)
                .build();
        EventHelper.processGCStatisticsData(GarmadonSerialization.getTypeName(1000), event, eventMaps);
        Assert.assertEquals(collector_name, eventMaps.get(GarmadonSerialization.getTypeName(1000)).get("collector_name"));
    }


    @Test
    public void processContainerResourceEvent_should_add_a_hashmap_with_limit() {
        long limit = 4831838208L;
        ContainerEventProtos.ContainerResourceEvent event = ContainerEventProtos.ContainerResourceEvent
                .newBuilder()
                .setType("MEMORY")
                .setLimit(limit)
                .setValue(2528430080L)
                .build();
        EventHelper.processContainerResourceEvent(GarmadonSerialization.getTypeName(2000), event, eventMaps);
        Assert.assertEquals(limit, eventMaps.get(GarmadonSerialization.getTypeName(2000)).get("limit"));
    }


    @Test
    public void processStageEvent_should_add_a_hashmap_with_jvm_gc_time() {
        long limit = 4831838208L;
        SparkEventProtos.StageEvent event = SparkEventProtos.StageEvent
                .newBuilder()
                .setStartTime(timestamp)
                .setStageName("stage0")
                .setStageId("0")
                .setAttemptId("0")
                .setNumTasks(1)
                .setStatus("success")
                .setExecutorCpuTime(1L)
                .setExecutorDeserializeCpuTime(1L)
                .setExecutorDeserializeTime(1L)
                .setExecutorRunTime(1l)
                .setResultSerializationTime(1L)
                .setResultSize(1L)
                .setPeakExecutionMemory(1L)
                .setDiskBytesSpilled(1L)
                .setMemoryBytesSpilled(1L)
                .setShuffleReadRecords(1L)
                .setShuffleReadFetchWaitTime(1L)
                .setShuffleReadLocalBytes(1L)
                .setShuffleReadRemoteBytes(1L)
                .setShuffleReadTotalBytes(1L)
                .setShuffleReadLocalBlocksFetched(1L)
                .setShuffleReadRemoteBlocksFetched(1L)
                .setShuffleReadTotalBlocksFetched(1L)
                .setShuffleWriteShuffleRecords(1L)
                .setShuffleWriteShuffleTime(1L)
                .setShuffleWriteShuffleBytes(1L)
                .setInputRecords(1L)
                .setInputBytes(1L)
                .setOutputRecords(1L)
                .setOutputBytes(1L)
                .setJvmGcTime(100L)
                .build();
        EventHelper.processStageEvent(GarmadonSerialization.getTypeName(3000), event, eventMaps);
        Assert.assertEquals(100L, eventMaps.get(GarmadonSerialization.getTypeName(3000)).get("jvm_gc_time"));
    }
}
