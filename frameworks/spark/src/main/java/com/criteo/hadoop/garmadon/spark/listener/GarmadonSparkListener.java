package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.schema.events.spark.StageEvent;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.util.function.Consumer;

public class GarmadonSparkListener extends SparkListener {
    public final Consumer<Object> eventHandler;

    public GarmadonSparkListener() {
        this.eventHandler = SparkListernerConf.getInstance().getEventHandler();
    }

    Function0<Long> zeroLongScala = new AbstractFunction0<Long>() {
        @Override
        public Long apply() {
            return 0L;
        }
    };
    Function0<String> emptyStringScala = new AbstractFunction0<String>() {
        @Override
        public String apply() {
            return "";
        }
    };

    // Stage Events
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {

    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        long submissionTime = stageCompleted.stageInfo().submissionTime().getOrElse(zeroLongScala);
        String name = stageCompleted.stageInfo().name();
        String stage_id = String.valueOf(stageCompleted.stageInfo().stageId());
        String attempt_id = String.valueOf(stageCompleted.stageInfo().attemptId());
        int numTasks = stageCompleted.stageInfo().numTasks();
        long completionTime = stageCompleted.stageInfo().completionTime().getOrElse(zeroLongScala);

        String status = stageCompleted.stageInfo().getStatusString();

        long executor_cpu_time = stageCompleted.stageInfo().taskMetrics().executorCpuTime();
        long executor_deserialize_cpu_time = stageCompleted.stageInfo().taskMetrics().executorDeserializeCpuTime();
        long executor_run_time = stageCompleted.stageInfo().taskMetrics().executorRunTime();
        long jvm_gc_time = stageCompleted.stageInfo().taskMetrics().jvmGCTime();
        long executor_deserialize_time = stageCompleted.stageInfo().taskMetrics().executorDeserializeTime();
        long result_serialization_time = stageCompleted.stageInfo().taskMetrics().resultSerializationTime();
        long result_size = stageCompleted.stageInfo().taskMetrics().resultSize();
        long peak_execution_memory = stageCompleted.stageInfo().taskMetrics().peakExecutionMemory();
        long disk_bytes_spilled = stageCompleted.stageInfo().taskMetrics().diskBytesSpilled();
        long memory_bytes_spilled = stageCompleted.stageInfo().taskMetrics().memoryBytesSpilled();
        long shuffle_read_records = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().recordsRead();
        long shuffle_read_fetch_wait_time = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().fetchWaitTime();
        long shuffle_read_local_bytes = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().localBytesRead();
        long shuffle_read_remote_bytes = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().remoteBytesRead();
        long shuffle_read_total_bytes = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBytesRead();
        long shuffle_read_local_blocks_fetched = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().localBlocksFetched();
        long shuffle_read_remote_blocks_fetched = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().remoteBlocksFetched();
        long shuffle_read_total_blocks_fetched = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBlocksFetched();
        long shuffle_write_shuffle_records = stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleRecordsWritten();
        long shuffle_write_shuffle_time = stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleWriteTime();
        long shuffle_write_shuffle_bytes = stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleBytesWritten();
        long input_records = stageCompleted.stageInfo().taskMetrics().inputMetrics().recordsRead();
        long input_bytes = stageCompleted.stageInfo().taskMetrics().inputMetrics().bytesRead();
        long output_records = stageCompleted.stageInfo().taskMetrics().outputMetrics().recordsWritten();
        long output_bytes = stageCompleted.stageInfo().taskMetrics().outputMetrics().bytesWritten();

        StageEvent stageEvent = new StageEvent(submissionTime, completionTime, name, stage_id,
                attempt_id, numTasks, status, executor_cpu_time, executor_deserialize_cpu_time,
                executor_run_time, jvm_gc_time, executor_deserialize_time, result_serialization_time,
                result_size, peak_execution_memory, disk_bytes_spilled, memory_bytes_spilled,
                shuffle_read_records, shuffle_read_fetch_wait_time, shuffle_read_local_bytes,
                shuffle_read_remote_bytes, shuffle_read_total_bytes, shuffle_read_local_blocks_fetched,
                shuffle_read_remote_blocks_fetched, shuffle_read_total_blocks_fetched, shuffle_write_shuffle_records,
                shuffle_write_shuffle_time, shuffle_write_shuffle_bytes, input_records, input_bytes,
                output_records, output_bytes);

        if (!status.equals("succeeded")) {
            String failureReason = stageCompleted.stageInfo().failureReason().getOrElse(emptyStringScala);
            stageEvent.setFailure_reason(failureReason);
        }

        this.eventHandler.accept(stageEvent);
    }
}
