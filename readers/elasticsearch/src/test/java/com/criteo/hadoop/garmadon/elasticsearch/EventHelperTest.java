package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EventHelperTest {
    HashMap<String, Map<String, Object>> eventMaps;

    @Before
    public void setUp() {
        eventMaps = new HashMap<>();
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

        // Empty section
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyEmpty = JVMStatisticsEventsProtos.JVMStatisticsData.Property
            .newBuilder()
            .setName("")
            .setValue("")
            .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionEmpty = JVMStatisticsEventsProtos.JVMStatisticsData.Section
            .newBuilder()
            .setName("class")
            .addProperty(propertyEmpty)
            .build();

        JVMStatisticsEventsProtos.JVMStatisticsData event = JVMStatisticsEventsProtos.JVMStatisticsData
            .newBuilder()
            .addSection(sectionDisk)
            .addSection(sectionNetwork)
            .addSection(sectionOther1)
            .addSection(sectionOther2)
            .addSection(sectionEmpty)
            .build();
        EventHelper.processJVMStatisticsData(GarmadonSerialization.getTypeName(1001), event, eventMaps);
        Assert.assertEquals(disk, eventMaps.get(disk).get("disk"));
        Assert.assertEquals(network, eventMaps.get(network).get("network"));
        Assert.assertEquals(499.0, eventMaps.get(GarmadonSerialization.getTypeName(1001)).get("memory_physical"));
        Assert.assertEquals(499.0, eventMaps.get(GarmadonSerialization.getTypeName(1001)).get("machinecpu_physical"));
        Assert.assertFalse(eventMaps.get(GarmadonSerialization.getTypeName(1001)).containsKey("class_"));
    }
}
