package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
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

    public static void processStageEvent(String type, SparkEventProtos.StageEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getCompletionTime());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
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

    public static void processTaskEvent(String type, SparkEventProtos.TaskEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getCompletionTime());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
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

    public static void processStageStateEvent(String type, SparkEventProtos.StageStateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("stage_name", event.getStageName());
        eventMap.put("stage_id", event.getStageId());
        eventMap.put("stage_attempt_id", event.getAttemptId());
        eventMap.put("num_tasks", event.getNumTasks());
        eventMap.put("state", event.getState());
    }

    public static void processExecutorStateEvent(String type, SparkEventProtos.ExecutorStateEvent event, HashMap<String, Map<String, Object>> eventMaps) {
        Date timestamp_date = new Date(event.getTimestamp());
        Map<String, Object> eventMap = eventMaps.computeIfAbsent(type, s -> EventHelper.initEvent(type, timestamp_date));
        eventMap.put("executor_hostname", event.getExecutorHostname());
        eventMap.put("reason", event.getReason());
        eventMap.put("task_failures", event.getTaskFailures());
        eventMap.put("state", event.getState());
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
