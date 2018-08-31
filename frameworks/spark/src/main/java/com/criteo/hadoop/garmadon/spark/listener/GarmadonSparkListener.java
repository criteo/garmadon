package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.State;
import org.apache.spark.scheduler.*;

import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.util.HashMap;
import java.util.function.Consumer;


// TODO: application name (keyword + better indexing + in the graph?)
public class GarmadonSparkListener extends SparkListener {
    public final Consumer<Object> eventHandler;

    private final HashMap<String, String> executorHostId = new HashMap<>();

    public GarmadonSparkListener() {
        this.eventHandler = SparkListernerConf.getInstance().getEventHandler();
    }

    private Function0<Long> zeroLongScala = new AbstractFunction0<Long>() {
        @Override
        public Long apply() {
            return 0L;
        }
    };
    private Function0<Long> currentTimeLongScala = new AbstractFunction0<Long>() {
        @Override
        public Long apply() {
            return System.currentTimeMillis();
        }
    };
    private Function0<String> emptyStringScala = new AbstractFunction0<String>() {
        @Override
        public String apply() {
            return "";
        }
    };

    private void sendStageStateEvent(long stateTime, State state, String name, String stageId,
                                     String attemptId, int numTasks) {
        DataAccessEventProtos.StateEvent stateEvent = DataAccessEventProtos.StateEvent
                .newBuilder()
                .setTimestamp(stateTime)
                .setState(state.name())
                .build();

        SparkEventProtos.StageStateEvent stageStateEvent = SparkEventProtos.StageStateEvent
                .newBuilder()
                .setStateEvent(stateEvent)
                .setStageName(name)
                .setStageId(stageId)
                .setAttemptId(attemptId)
                .setNumTasks(numTasks)
                .build();

        this.eventHandler.accept(stageStateEvent);
    }

    private void sendExecutorStateEvent(long time, State state, String executorId, String executorHost
            , String reason, int taskFailures) {
        DataAccessEventProtos.StateEvent stateEvent = DataAccessEventProtos.StateEvent
                .newBuilder()
                .setTimestamp(time)
                .setState(state.name())
                .build();

        SparkEventProtos.ExecutorStateEvent.Builder executorStateEvent = SparkEventProtos.ExecutorStateEvent
                .newBuilder()
                .setStateEvent(stateEvent)
                .setExecutorId(executorId)
                .setExecutorHostname(executorHost);

        if (reason != null) {
            executorStateEvent.setReason(reason);
        }

        if (taskFailures != 0) {
            executorStateEvent.setTaskFailures(taskFailures);
        }

        this.eventHandler.accept(executorStateEvent.build());
    }


    // Stage Events
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        long submissionTime = stageSubmitted.stageInfo().submissionTime().getOrElse(currentTimeLongScala);
        String name = stageSubmitted.stageInfo().name();
        String stageId = String.valueOf(stageSubmitted.stageInfo().stageId());
        String attemptId = String.valueOf(stageSubmitted.stageInfo().attemptId());
        int numTasks = stageSubmitted.stageInfo().numTasks();

        sendStageStateEvent(submissionTime, State.BEGIN, name, stageId, attemptId, numTasks);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        long submissionTime = stageCompleted.stageInfo().submissionTime().getOrElse(zeroLongScala);
        long completionTime = stageCompleted.stageInfo().completionTime().getOrElse(currentTimeLongScala);
        String name = stageCompleted.stageInfo().name();
        String stageId = String.valueOf(stageCompleted.stageInfo().stageId());
        String attemptId = String.valueOf(stageCompleted.stageInfo().attemptId());
        int numTasks = stageCompleted.stageInfo().numTasks();

        sendStageStateEvent(completionTime, State.END, name, stageId, attemptId, numTasks);

        String status = stageCompleted.stageInfo().getStatusString();

        SparkEventProtos.StageEvent.Builder stageEventBuilder = SparkEventProtos.StageEvent
                .newBuilder()
                .setStartTime(submissionTime)
                .setCompletionTime(completionTime)
                .setStageName(name)
                .setStageId(stageId)
                .setAttemptId(attemptId)
                .setNumTasks(numTasks)
                .setStatus(status)
                .setExecutorCpuTime(stageCompleted.stageInfo().taskMetrics().executorCpuTime())
                .setExecutorDeserializeCpuTime(stageCompleted.stageInfo().taskMetrics().executorDeserializeCpuTime())
                .setExecutorRunTime(stageCompleted.stageInfo().taskMetrics().executorRunTime())
                .setJvmGcTime(stageCompleted.stageInfo().taskMetrics().jvmGCTime())
                .setExecutorDeserializeTime(stageCompleted.stageInfo().taskMetrics().executorDeserializeTime())
                .setResultSerializationTime(stageCompleted.stageInfo().taskMetrics().resultSerializationTime())
                .setResultSize(stageCompleted.stageInfo().taskMetrics().resultSize())
                .setPeakExecutionMemory(stageCompleted.stageInfo().taskMetrics().peakExecutionMemory())
                .setDiskBytesSpilled(stageCompleted.stageInfo().taskMetrics().diskBytesSpilled())
                .setMemoryBytesSpilled(stageCompleted.stageInfo().taskMetrics().memoryBytesSpilled())
                .setShuffleReadRecords(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().recordsRead())
                .setShuffleReadFetchWaitTime(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().fetchWaitTime())
                .setShuffleReadLocalBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().localBytesRead())
                .setShuffleReadRemoteBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().remoteBytesRead())
                .setShuffleReadTotalBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBytesRead())
                .setShuffleReadLocalBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().localBlocksFetched())
                .setShuffleReadRemoteBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().remoteBlocksFetched())
                .setShuffleReadTotalBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBlocksFetched())
                .setShuffleWriteShuffleRecords(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleRecordsWritten())
                .setShuffleWriteShuffleTime(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleWriteTime())
                .setShuffleWriteShuffleBytes(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleBytesWritten())
                .setInputRecords(stageCompleted.stageInfo().taskMetrics().inputMetrics().recordsRead())
                .setInputBytes(stageCompleted.stageInfo().taskMetrics().inputMetrics().bytesRead())
                .setOutputRecords(stageCompleted.stageInfo().taskMetrics().outputMetrics().recordsWritten())
                .setOutputBytes(stageCompleted.stageInfo().taskMetrics().outputMetrics().bytesWritten());

        if (!status.equals("succeeded")) {
            stageEventBuilder.setFailureReason(stageCompleted.stageInfo().failureReason().getOrElse(emptyStringScala));
        }

        this.eventHandler.accept(stageEventBuilder.build());
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        String status = taskEnd.taskInfo().status();

        SparkEventProtos.TaskEvent.Builder taskEventBuilder = SparkEventProtos.TaskEvent
                .newBuilder()
                .setStartTime(taskEnd.taskInfo().launchTime())
                .setCompletionTime(taskEnd.taskInfo().finishTime())
                .setTaskId(String.valueOf(taskEnd.taskInfo().taskId()))
                .setStageId(String.valueOf(taskEnd.stageId()))
                .setAttemptId(String.valueOf(taskEnd.stageAttemptId()))
                .setExecutorId(taskEnd.taskInfo().executorId())
                .setExecutorHostname(String.valueOf(taskEnd.taskInfo().host()))
                .setStatus(status)
                .setLocality(taskEnd.taskInfo().taskLocality().toString())
                .setType(taskEnd.taskType())
                .setAttemptNumber(taskEnd.taskInfo().attemptNumber())
                .setExecutorCpuTime(taskEnd.taskMetrics().executorCpuTime())
                .setExecutorDeserializeCpuTime(taskEnd.taskMetrics().executorDeserializeCpuTime())
                .setExecutorRunTime(taskEnd.taskMetrics().executorRunTime())
                .setJvmGcTime(taskEnd.taskMetrics().jvmGCTime())
                .setExecutorDeserializeTime(taskEnd.taskMetrics().executorDeserializeTime())
                .setResultSerializationTime(taskEnd.taskMetrics().resultSerializationTime())
                .setResultSize(taskEnd.taskMetrics().resultSize())
                .setPeakExecutionMemory(taskEnd.taskMetrics().peakExecutionMemory())
                .setDiskBytesSpilled(taskEnd.taskMetrics().diskBytesSpilled())
                .setMemoryBytesSpilled(taskEnd.taskMetrics().memoryBytesSpilled())
                .setShuffleReadRecords(taskEnd.taskMetrics().shuffleReadMetrics().recordsRead())
                .setShuffleReadFetchWaitTime(taskEnd.taskMetrics().shuffleReadMetrics().fetchWaitTime())
                .setShuffleReadLocalBytes(taskEnd.taskMetrics().shuffleReadMetrics().localBytesRead())
                .setShuffleReadRemoteBytes(taskEnd.taskMetrics().shuffleReadMetrics().remoteBytesRead())
                .setShuffleReadTotalBytes(taskEnd.taskMetrics().shuffleReadMetrics().totalBytesRead())
                .setShuffleReadLocalBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().localBlocksFetched())
                .setShuffleReadRemoteBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().remoteBlocksFetched())
                .setShuffleReadTotalBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().totalBlocksFetched())
                .setShuffleWriteShuffleRecords(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleRecordsWritten())
                .setShuffleWriteShuffleTime(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleWriteTime())
                .setShuffleWriteShuffleBytes(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleBytesWritten())
                .setInputRecords(taskEnd.taskMetrics().inputMetrics().recordsRead())
                .setInputBytes(taskEnd.taskMetrics().inputMetrics().bytesRead())
                .setOutputRecords(taskEnd.taskMetrics().outputMetrics().recordsWritten())
                .setOutputBytes(taskEnd.taskMetrics().outputMetrics().bytesWritten());


        if (!status.equals("succeeded")) {
            taskEventBuilder.setFailureReason(taskEnd.reason().toString());
        }

        this.eventHandler.accept(taskEventBuilder.build());
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        executorHostId.put(executorAdded.executorId(), executorAdded.executorInfo().executorHost());
        sendExecutorStateEvent(executorAdded.time(),
                State.ADDED,
                executorAdded.executorId(),
                executorAdded.executorInfo().executorHost(),
                null,
                0);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        sendExecutorStateEvent(executorRemoved.time(),
                State.REMOVED,
                executorRemoved.executorId(),
                executorHostId.getOrDefault(executorRemoved.executorId(), "UNKNOWN"),
                executorRemoved.reason(),
                0);
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        sendExecutorStateEvent(executorBlacklisted.time(),
                State.BLACKLISTED,
                executorBlacklisted.executorId(),
                executorHostId.getOrDefault(executorBlacklisted.executorId(), "UNKNOWN"),
                null,
                executorBlacklisted.taskFailures());
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        sendExecutorStateEvent(executorUnblacklisted.time(),
                State.UNBLACKLISTED,
                executorUnblacklisted.executorId(),
                executorHostId.getOrDefault(executorUnblacklisted.executorId(), "UNKNOWN"),
                null,
                0);
    }
}
