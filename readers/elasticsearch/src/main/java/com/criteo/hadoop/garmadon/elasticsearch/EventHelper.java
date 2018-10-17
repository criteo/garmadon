package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.jvm.JVMStatisticsProtos;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

class EventHelper {
    static Map<String, Object> initEvent(String type) {
        HashMap<String, Object> json = new HashMap<>();
        json.put("event_type", type);
        return json;
    }

    static void processPathEvent(String type, DataAccessEventProtos.PathEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("type", event.getType());
        eventMap.put("path", event.getPath());
    }

    static void processFsEvent(String type, DataAccessEventProtos.FsEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        String uri = event.getUri();
        eventMap.put("src_path", event.getSrcPath().replace(uri, ""));
        eventMap.put("dst_path", event.getDstPath().replace(uri, ""));
        eventMap.put("action", event.getAction());
        eventMap.put("uri", UriHelper.getUniformizedUri(uri));
        eventMap.put("method_duration_millis", event.getMethodDurationMillis());
    }

    static void processStateEvent(String type, DataAccessEventProtos.StateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("state", event.getState());
    }

    static void processJVMStatisticsData(String type, JVMStatisticsProtos.JVMStatisticsData event, HashMap<String, Map<String, Object>> eventMaps) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : event.getSectionList()) {
            if (section.getName().equals("disk") || section.getName().equals("network")) {
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    String[] device = property.getName().split("_");
                    if (device.length == 2) {
                        Map<String, Object> eventMap = eventMaps.computeIfAbsent(device[0], s -> EventHelper.initEvent("OS"));
                        eventMap.put(section.getName(), device[0]);
                        eventMap.put(device[1], Double.parseDouble(property.getValue()));
                    }
                }
            } else {
                Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
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

    static void processStageEvent(String type, SparkEventProtos.StageEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("start_time", new Date(event.getStartTime()));
        eventMap.put("stage_name", event.getStageName());
        eventMap.put("stage_id", event.getStageId());
        eventMap.put("stage_attempt_id", event.getAttemptId());
        eventMap.put("num_tasks", event.getNumTasks());

        eventMap.put("status", event.getStatus());
        eventMap.put("failure_reason", event.getFailureReason());
        eventMap.put("executor_cpu_time", event.getExecutorCpuTime());
        eventMap.put("executor_deserialize_cpu_time", event.getExecutorDeserializeCpuTime());
        eventMap.put("executor_run_time", event.getExecutorRunTime());
        eventMap.put("jvm_gc_time", event.getJvmGcTime());
        eventMap.put("executor_deserialize_time", event.getExecutorDeserializeTime());
        eventMap.put("result_serialization_time", event.getResultSerializationTime());
        eventMap.put("result_size", event.getResultSize());
        eventMap.put("peak_execution_memory", event.getPeakExecutionMemory());
        eventMap.put("disk_bytes_spilled", event.getDiskBytesSpilled());
        eventMap.put("memory_bytes_spilled", event.getMemoryBytesSpilled());
        eventMap.put("shuffle_read_records", event.getShuffleReadRecords());
        eventMap.put("shuffle_read_fetch_wait_time", event.getShuffleReadFetchWaitTime());
        eventMap.put("shuffle_read_local_bytes", event.getShuffleReadLocalBytes());
        eventMap.put("shuffle_read_remote_bytes", event.getShuffleReadRemoteBytes());
        eventMap.put("shuffle_read_total_bytes", event.getShuffleReadTotalBytes());
        eventMap.put("shuffle_read_local_blocks_fetched", event.getShuffleReadLocalBlocksFetched());
        eventMap.put("shuffle_read_remote_blocks_fetched", event.getShuffleReadRemoteBlocksFetched());
        eventMap.put("shuffle_read_total_blocks_fetched", event.getShuffleReadTotalBlocksFetched());
        eventMap.put("shuffle_write_shuffle_records", event.getShuffleWriteShuffleRecords());
        eventMap.put("shuffle_write_shuffle_time", event.getShuffleWriteShuffleTime());
        eventMap.put("shuffle_write_shuffle_bytes", event.getShuffleWriteShuffleBytes());
        eventMap.put("input_records", event.getInputRecords());
        eventMap.put("input_bytes", event.getInputBytes());
        eventMap.put("output_records", event.getOutputRecords());
        eventMap.put("output_bytes", event.getOutputBytes());
    }

    static void processTaskEvent(String type, SparkEventProtos.TaskEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("start_time", new Date(event.getStartTime()));
        eventMap.put("task_id", event.getTaskId());
        eventMap.put("stage_id", event.getStageId());
        eventMap.put("stage_attempt_id", event.getAttemptId());
        eventMap.put("executor_hostname", event.getExecutorHostname());

        eventMap.put("status", event.getStatus());
        eventMap.put("failure_reason", event.getFailureReason());
        eventMap.put("executor_cpu_time", event.getExecutorCpuTime());
        eventMap.put("executor_deserialize_cpu_time", event.getExecutorDeserializeCpuTime());
        eventMap.put("executor_run_time", event.getExecutorRunTime());
        eventMap.put("jvm_gc_time", event.getJvmGcTime());
        eventMap.put("executor_deserialize_time", event.getExecutorDeserializeTime());
        eventMap.put("result_serialization_time", event.getResultSerializationTime());
        eventMap.put("result_size", event.getResultSize());
        eventMap.put("peak_execution_memory", event.getPeakExecutionMemory());
        eventMap.put("disk_bytes_spilled", event.getDiskBytesSpilled());
        eventMap.put("memory_bytes_spilled", event.getMemoryBytesSpilled());
        eventMap.put("shuffle_read_records", event.getShuffleReadRecords());
        eventMap.put("shuffle_read_fetch_wait_time", event.getShuffleReadFetchWaitTime());
        eventMap.put("shuffle_read_local_bytes", event.getShuffleReadLocalBytes());
        eventMap.put("shuffle_read_remote_bytes", event.getShuffleReadRemoteBytes());
        eventMap.put("shuffle_read_total_bytes", event.getShuffleReadTotalBytes());
        eventMap.put("shuffle_read_local_blocks_fetched", event.getShuffleReadLocalBlocksFetched());
        eventMap.put("shuffle_read_remote_blocks_fetched", event.getShuffleReadRemoteBlocksFetched());
        eventMap.put("shuffle_read_total_blocks_fetched", event.getShuffleReadTotalBlocksFetched());
        eventMap.put("shuffle_write_shuffle_records", event.getShuffleWriteShuffleRecords());
        eventMap.put("shuffle_write_shuffle_time", event.getShuffleWriteShuffleTime());
        eventMap.put("shuffle_write_shuffle_bytes", event.getShuffleWriteShuffleBytes());
        eventMap.put("input_records", event.getInputRecords());
        eventMap.put("input_bytes", event.getInputBytes());
        eventMap.put("output_records", event.getOutputRecords());
        eventMap.put("output_bytes", event.getOutputBytes());

        eventMap.put("locality", event.getLocality());

        eventMap.put("type", event.getType());

        eventMap.put("attempt_number", event.getAttemptNumber());
    }

    static void processStageStateEvent(String type, SparkEventProtos.StageStateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("stage_name", event.getStageName());
        eventMap.put("stage_id", event.getStageId());
        eventMap.put("stage_attempt_id", event.getAttemptId());
        eventMap.put("num_tasks", event.getNumTasks());
        eventMap.put("state", event.getState());
    }

    static void processExecutorStateEvent(String type, SparkEventProtos.ExecutorStateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("executor_hostname", event.getExecutorHostname());
        eventMap.put("reason", event.getReason());
        eventMap.put("task_failures", event.getTaskFailures());
        eventMap.put("state", event.getState());
    }

    static void processGCStatisticsData(String type, JVMStatisticsProtos.GCStatisticsData event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("collector_name", event.getCollectorName());
        eventMap.put("pause_time", event.getPauseTime());
        eventMap.put("cause", event.getCause());
        eventMap.put("delta_eden", event.getEdenBefore() - event.getEdenAfter());
        eventMap.put("delta_survivor", event.getSurvivorBefore() - event.getSurvivorAfter());
        eventMap.put("delta_old", event.getOldBefore() - event.getOldAfter());
        eventMap.put("delta_code", event.getCodeBefore() - event.getCodeAfter());
        eventMap.put("delta_metaspace", event.getMetaspaceBefore() - event.getMetaspaceAfter());
    }

    static void processContainerResourceEvent(String type, ContainerEventProtos.ContainerResourceEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("type", event.getType());
        eventMap.put("value", event.getValue());
        eventMap.put("limit", event.getLimit());
    }

    public static void processApplicationEvent(String type, ResourceManagerEventProtos.ApplicationEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type));
        eventMap.put("queue", event.getQueue());
        eventMap.put("state", event.getState());
        eventMap.put("tracking_url", event.getTrackingUrl());
        eventMap.put("original_tracking_url", event.getOriginalTrackingUrl());
    }
}
