package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

class EventHelper {

    protected EventHelper() {
        throw new UnsupportedOperationException();
    }

    static Map<String, Object> initEvent(String type) {
        HashMap<String, Object> json = new HashMap<>();
        json.put("event_type", type);
        return json;
    }

    static void processJVMStatisticsData(String type, JVMStatisticsEventsProtos.JVMStatisticsData event, Map<String, Map<String, Object>> eventMaps) {
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : event.getSectionList()) {
            if (section.getName().equals("disk") || section.getName().equals("network")) {
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    String[] device = property.getName().split("_");
                    if (device.length == 2) {
                        Map<String, Object> eventMap = eventMaps.computeIfAbsent(device[0], s -> EventHelper.initEvent("OS"));
                        eventMap.put(section.getName(), device[0]);
                        eventMap.put(device[1], Double.parseDouble(property.getValue()));
                    }
                }
            } else {
                Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    try {
                        eventMap.put(section.getName() + "_" + property.getName(), Double.parseDouble(property.getValue()));
                    } catch (NumberFormatException nfe) {
                        eventMap.put(section.getName() + "_" + property.getName(), property.getValue());
                    }
                }
            }
        }
    }
}
