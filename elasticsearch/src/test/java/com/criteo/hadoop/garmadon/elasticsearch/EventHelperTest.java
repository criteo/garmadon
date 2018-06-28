package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.jvm.JVMStatisticsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
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
        Map<String, Object> res = EventHelper.initEvent(type, new Date(timestamp));
        Assert.assertEquals(type, res.get("type"));
    }

    @Test
    public void processPathEvent_should_add_a_hashmap_with_path() {
        String path = "datadisco:/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.PathEvent event = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setType("INPUT")
                .setPath(path)
                .build();
        EventHelper.processPathEvent(event, eventMaps);
        Assert.assertEquals(path, eventMaps.get("INPUT").get("path"));
    }

    @Test
    public void processFsEvent_should_add_a_hashmap_with_dst_path() {
        String path = "/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setAction("RENAME")
                .setDstPath(path)
                .setUri("hdfs://root")
                .build();
        EventHelper.processFsEvent(event, eventMaps);
        Assert.assertEquals(path, eventMaps.get("FS").get("dst_path"));
    }

    @Test
    public void processFsEvent_remove_uri_from_dst_payj() {
        String uri = "hdfs://root";
        String path = uri + "/click_cas/day=2018-06-26/hour=03/platform=EU";
        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setAction("RENAME")
                .setDstPath(path)
                .setUri(uri)
                .build();
        EventHelper.processFsEvent(event, eventMaps);
        Assert.assertEquals(path.replace(uri, ""), eventMaps.get("FS").get("dst_path"));
    }

    @Test
    public void processStateEvent_should_add_a_hashmap_with_state() {
        String state = "END";
        DataAccessEventProtos.StateEvent event = DataAccessEventProtos.StateEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setState(state)
                .build();
        EventHelper.processStateEvent(event, eventMaps);
        Assert.assertEquals(state, eventMaps.get("STATE").get("state"));
    }


    @Test
    public void processJVMStatisticsData_should_add_3_hashmap_disk_network_and_jvm() {
        // Disk section
        String disk = "sda";
        JVMStatisticsProtos.JVMStatisticsData.Property propertyDisk = JVMStatisticsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName(disk + "_write")
                .setValue("10000")
                .build();
        JVMStatisticsProtos.JVMStatisticsData.Section sectionDisk = JVMStatisticsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("disk")
                .addProperty(propertyDisk)
                .build();

        // Network Section
        String network = "enp129s0";
        JVMStatisticsProtos.JVMStatisticsData.Property propertyNetwork = JVMStatisticsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName(network + "_rx")
                .setValue("100000")
                .build();
        JVMStatisticsProtos.JVMStatisticsData.Section sectionNetwork = JVMStatisticsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("network")
                .addProperty(propertyNetwork)
                .build();


        // Others Section
        JVMStatisticsProtos.JVMStatisticsData.Property propertyOther = JVMStatisticsProtos.JVMStatisticsData.Property
                .newBuilder()
                .setName("physical")
                .setValue("499")
                .build();
        JVMStatisticsProtos.JVMStatisticsData.Section sectionOther1 = JVMStatisticsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("memory")
                .addProperty(propertyOther)
                .build();
        JVMStatisticsProtos.JVMStatisticsData.Section sectionOther2 = JVMStatisticsProtos.JVMStatisticsData.Section
                .newBuilder()
                .setName("machinecpu")
                .addProperty(propertyOther)
                .build();

        JVMStatisticsProtos.JVMStatisticsData event = JVMStatisticsProtos.JVMStatisticsData
                .newBuilder()
                .setTimestamp(timestamp)
                .addSection(sectionDisk)
                .addSection(sectionNetwork)
                .addSection(sectionOther1)
                .addSection(sectionOther2)
                .build();
        EventHelper.processJVMStatisticsData(event, eventMaps);
        Assert.assertEquals(disk, eventMaps.get(disk).get("disk"));
        Assert.assertEquals(network, eventMaps.get(network).get("network"));
        Assert.assertEquals(499.0, eventMaps.get("JVM").get("memory_physical"));
        Assert.assertEquals(499.0, eventMaps.get("JVM").get("machinecpu_physical"));
    }


    @Test
    public void processGCStatisticsData_should_add_a_hashmap_with_path() {
        String collector_name = "PS Scavenge";
        JVMStatisticsProtos.GCStatisticsData event = JVMStatisticsProtos.GCStatisticsData
                .newBuilder()
                .setTimestamp(timestamp)
                .setCause("Allocation Failure")
                .setCollectorName(collector_name)
                .build();
        EventHelper.processGCStatisticsData(event, eventMaps);
        Assert.assertEquals(collector_name, eventMaps.get("GC").get("collector_name"));
    }


    @Test
    public void processContainerResourceEvent_should_add_a_hashmap_with_limith() {
        long limit = 4831838208L;
        ContainerEventProtos.ContainerResourceEvent event = ContainerEventProtos.ContainerResourceEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setType("MEMORY")
                .setLimit(limit)
                .setValue(2528430080L)
                .build();
        EventHelper.processContainerResourceEvent(event, eventMaps);
        Assert.assertEquals(limit, eventMaps.get("MEMORY").get("limit"));
    }
}
