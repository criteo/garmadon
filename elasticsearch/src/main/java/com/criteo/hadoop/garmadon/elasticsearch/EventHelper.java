package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.jvm.JVMStatisticsProtos;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventHelper {
    public static Map<String, Object> initEvent(String type, Date timestamp_date) {
        HashMap<String, Object> json = new HashMap<>();
        json.put("timestamp", timestamp_date);
        json.put("event_type", type);
        return json;
    }

    public static void processPathEvent(String type, DataAccessEventProtos.PathEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("type", event.getType());
        eventMap.put("path", event.getPath());
    }

    public static void processFsEvent(String type, DataAccessEventProtos.FsEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        String uri = event.getUri();
        eventMap.put("src_path", event.getSrcPath().replace(uri, ""));
        eventMap.put("dst_path", event.getDstPath().replace(uri, ""));
        eventMap.put("action", event.getAction());
        eventMap.put("uri", UriHelper.getUniformizedUri(uri));
    }

    public static void processStateEvent(String type, DataAccessEventProtos.StateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("state", event.getState());
    }

    public static void processJVMStatisticsData(String type, JVMStatisticsProtos.JVMStatisticsData event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());

        for (JVMStatisticsProtos.JVMStatisticsData.Section section : event.getSectionList()) {
            if (section.getName().equals("disk") || section.getName().equals("network")) {
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    String[] device = property.getName().split("_");
                    if (device.length == 2) {
                        Map<String, Object> eventMap = eventMaps.computeIfAbsent(device[0], s -> EventHelper.initEvent("OS",
                                timestamp_date));
                        eventMap.put(section.getName(), device[0]);
                        eventMap.put(device[1], Double.parseDouble(property.getValue()));
                    }
                }
            } else {
                Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    try {
                        eventMap.put(section.getName() + "_" + property.getName(), Double.parseDouble(property.getValue()));
                    } catch (NumberFormatException nfe) {
                        eventMap.put(section.getName() + "_" + property.getName(), property.getValue());
                    }
                }
            }
        }
    }

    public static void processGCStatisticsData(String type, JVMStatisticsProtos.GCStatisticsData event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("collector_name", event.getCollectorName());
        eventMap.put("pause_time", event.getPauseTime());
        eventMap.put("cause", event.getCause());
        eventMap.put("delta_eden", event.getEdenBefore() - event.getEdenAfter());
        eventMap.put("delta_survivor", event.getSurvivorBefore() - event.getSurvivorAfter());
        eventMap.put("delta_old", event.getOldBefore() - event.getOldAfter());
        eventMap.put("delta_code", event.getCodeBefore() - event.getCodeAfter());
        eventMap.put("delta_metaspace", event.getMetaspaceBefore() - event.getMetaspaceAfter());
    }

    public static void processContainerResourceEvent(String type, ContainerEventProtos.ContainerResourceEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("type", event.getType());
        eventMap.put("value", event.getValue());
        eventMap.put("limit", event.getLimit());
    }
}
