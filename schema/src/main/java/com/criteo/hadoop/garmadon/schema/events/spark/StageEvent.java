package com.criteo.hadoop.garmadon.schema.events.spark;

import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;

import java.util.Objects;


// TODO compute duration and do not put start_time?
public class StageEvent {
    private final long start_time;
    private final long completion_time;
    private final String stage_name;
    private final String stage_id;
    private final String attempt_id;
    private final int num_tasks;

    private final String status;
    private String failure_reason = null;

    private final long executor_cpu_time;
    private final long executor_deserialize_cpu_time;
    private final long executor_run_time;
    private final long jvm_gc_time;
    private final long executor_deserialize_time;
    private final long result_serialization_time;
    private final long result_size;
    private final long peak_execution_memory;
    private final long disk_bytes_spilled;
    private final long memory_bytes_spilled;
    private final long shuffle_read_records;
    private final long shuffle_read_fetch_wait_time;
    private final long shuffle_read_local_bytes;
    private final long shuffle_read_remote_bytes;
    private final long shuffle_read_total_bytes;
    private final long shuffle_read_local_blocks_fetched;
    private final long shuffle_read_remote_blocks_fetched;
    private final long shuffle_read_total_blocks_fetched;
    private final long shuffle_write_shuffle_records;
    private final long shuffle_write_shuffle_time;
    private final long shuffle_write_shuffle_bytes;
    private final long input_records;
    private final long input_bytes;
    private final long output_records;
    private final long output_bytes;

    public StageEvent(long start_time, long completion_time, String stage_name, String stage_id, String attempt_id,
                      int num_tasks, String status, long executor_cpu_time, long executor_deserialize_cpu_time,
                      long executor_run_time, long jvm_gc_time, long executor_deserialize_time, long result_serialization_time,
                      long result_size, long peak_execution_memory, long disk_bytes_spilled, long memory_bytes_spilled,
                      long shuffle_read_records, long shuffle_read_fetch_wait_time, long shuffle_read_local_bytes,
                      long shuffle_read_remote_bytes, long shuffle_read_total_bytes, long shuffle_read_local_blocks_fetched,
                      long shuffle_read_remote_blocks_fetched, long shuffle_read_total_blocks_fetched, long shuffle_write_shuffle_records,
                      long shuffle_write_shuffle_time, long shuffle_write_shuffle_bytes, long input_records, long input_bytes,
                      long output_records, long output_bytes) {
        this.start_time = start_time;
        this.completion_time = completion_time;
        this.stage_name = stage_name;
        this.stage_id = stage_id;
        this.attempt_id = attempt_id;
        this.num_tasks = num_tasks;

        this.status = status;

        this.executor_cpu_time = executor_cpu_time;
        this.executor_deserialize_cpu_time = executor_deserialize_cpu_time;
        this.executor_run_time = executor_run_time;
        this.jvm_gc_time = jvm_gc_time;
        this.executor_deserialize_time = executor_deserialize_time;
        this.result_serialization_time = result_serialization_time;
        this.result_size = result_size;
        this.peak_execution_memory = peak_execution_memory;
        this.disk_bytes_spilled = disk_bytes_spilled;
        this.memory_bytes_spilled = memory_bytes_spilled;
        this.shuffle_read_records = shuffle_read_records;
        this.shuffle_read_fetch_wait_time = shuffle_read_fetch_wait_time;
        this.shuffle_read_local_bytes = shuffle_read_local_bytes;
        this.shuffle_read_remote_bytes = shuffle_read_remote_bytes;
        this.shuffle_read_total_bytes = shuffle_read_total_bytes;
        this.shuffle_read_local_blocks_fetched = shuffle_read_local_blocks_fetched;
        this.shuffle_read_remote_blocks_fetched = shuffle_read_remote_blocks_fetched;
        this.shuffle_read_total_blocks_fetched = shuffle_read_total_blocks_fetched;
        this.shuffle_write_shuffle_records = shuffle_write_shuffle_records;
        this.shuffle_write_shuffle_time = shuffle_write_shuffle_time;
        this.shuffle_write_shuffle_bytes = shuffle_write_shuffle_bytes;
        this.input_records = input_records;
        this.input_bytes = input_bytes;
        this.output_records = output_records;
        this.output_bytes = output_bytes;
    }

    public byte[] serialize() {
        SparkEventProtos.StageEvent.Builder builder = SparkEventProtos.StageEvent
                .newBuilder()
                .setStartTime(this.start_time)
                .setCompletionTime(completion_time)
                .setStageName(this.stage_name)
                .setStageId(this.stage_id)
                .setAttemptId(this.attempt_id)
                .setNumTasks(this.num_tasks)
                .setStatus(this.status)
                .setExecutorCpuTime(executor_cpu_time)
                .setExecutorDeserializeCpuTime(executor_deserialize_cpu_time)
                .setExecutorRunTime(executor_run_time)
                .setJvmGcTime(jvm_gc_time)
                .setExecutorDeserializeTime(executor_deserialize_time)
                .setResultSerializationTime(result_serialization_time)
                .setResultSize(result_size)
                .setPeakExecutionMemory(peak_execution_memory)
                .setDiskBytesSpilled(disk_bytes_spilled)
                .setMemoryBytesSpilled(memory_bytes_spilled)
                .setShuffleReadRecords(shuffle_read_records)
                .setShuffleReadFetchWaitTime(shuffle_read_fetch_wait_time)
                .setShuffleReadLocalBytes(shuffle_read_local_bytes)
                .setShuffleReadRemoteBytes(shuffle_read_remote_bytes)
                .setShuffleReadTotalBytes(shuffle_read_total_bytes)
                .setShuffleReadLocalBlocksFetched(shuffle_read_local_blocks_fetched)
                .setShuffleReadRemoteBlocksFetched(shuffle_read_remote_blocks_fetched)
                .setShuffleReadTotalBlocksFetched(shuffle_read_total_blocks_fetched)
                .setShuffleWriteShuffleRecords(shuffle_write_shuffle_records)
                .setShuffleWriteShuffleTime(shuffle_write_shuffle_time)
                .setShuffleWriteShuffleBytes(shuffle_write_shuffle_bytes)
                .setInputRecords(input_records)
                .setInputBytes(input_bytes)
                .setOutputRecords(output_records)
                .setOutputBytes(output_bytes);

        if (failure_reason != null) {
            builder.setFailureReason(failure_reason);
        }

        return builder.build().toByteArray();
    }

    public void setFailure_reason(String failure_reason) {
        this.failure_reason = failure_reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StageEvent)) return false;
        StageEvent that = (StageEvent) o;
        return start_time == that.start_time &&
                completion_time == that.completion_time &&
                stage_id == that.stage_id &&
                attempt_id == that.attempt_id &&
                num_tasks == that.num_tasks &&
                Objects.equals(stage_name, that.stage_name) &&
                Objects.equals(status, that.status) &&
                Objects.equals(failure_reason, that.failure_reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start_time, completion_time, stage_name, stage_id, attempt_id, num_tasks, status, failure_reason, executor_cpu_time, executor_deserialize_cpu_time, executor_run_time, jvm_gc_time, executor_deserialize_time, result_serialization_time, result_size, peak_execution_memory, disk_bytes_spilled, memory_bytes_spilled, shuffle_read_records, shuffle_read_fetch_wait_time, shuffle_read_local_bytes, shuffle_read_remote_bytes, shuffle_read_total_bytes, shuffle_read_local_blocks_fetched, shuffle_read_remote_blocks_fetched, shuffle_read_total_blocks_fetched, shuffle_write_shuffle_records, shuffle_write_shuffle_time, shuffle_write_shuffle_bytes, input_records, input_bytes, output_records, output_bytes);
    }

    @Override
    public String toString() {
        return "StageEvent{" +
                "start_time=" + start_time +
                ", completion_time=" + completion_time +
                ", stage_name='" + stage_name + '\'' +
                ", stage_id='" + stage_id + '\'' +
                ", attempt_id='" + attempt_id + '\'' +
                ", num_tasks=" + num_tasks +
                ", status='" + status + '\'' +
                ", failure_reason='" + failure_reason + '\'' +
                ", executor_cpu_time=" + executor_cpu_time +
                ", executor_deserialize_cpu_time=" + executor_deserialize_cpu_time +
                ", executor_run_time=" + executor_run_time +
                ", jvm_gc_time=" + jvm_gc_time +
                ", executor_deserialize_time=" + executor_deserialize_time +
                ", result_serialization_time=" + result_serialization_time +
                ", result_size=" + result_size +
                ", peak_execution_memory=" + peak_execution_memory +
                ", disk_bytes_spilled=" + disk_bytes_spilled +
                ", memory_bytes_spilled=" + memory_bytes_spilled +
                ", shuffle_read_records=" + shuffle_read_records +
                ", shuffle_read_fetch_wait_time=" + shuffle_read_fetch_wait_time +
                ", shuffle_read_local_bytes=" + shuffle_read_local_bytes +
                ", shuffle_read_remote_bytes=" + shuffle_read_remote_bytes +
                ", shuffle_read_total_bytes=" + shuffle_read_total_bytes +
                ", shuffle_read_local_blocks_fetched=" + shuffle_read_local_blocks_fetched +
                ", shuffle_read_remote_blocks_fetched=" + shuffle_read_remote_blocks_fetched +
                ", shuffle_read_total_blocks_fetched=" + shuffle_read_total_blocks_fetched +
                ", shuffle_write_shuffle_records=" + shuffle_write_shuffle_records +
                ", shuffle_write_shuffle_time=" + shuffle_write_shuffle_time +
                ", shuffle_write_shuffle_bytes=" + shuffle_write_shuffle_bytes +
                ", input_records=" + input_records +
                ", input_bytes=" + input_bytes +
                ", output_records=" + output_records +
                ", output_bytes=" + output_bytes +
                '}';
    }
}
