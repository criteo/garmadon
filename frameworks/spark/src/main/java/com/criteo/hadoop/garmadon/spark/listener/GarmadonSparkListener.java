package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.util.HashMap;
import java.util.function.Supplier;


public class GarmadonSparkListener extends SparkListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonSparkListener.class);
    private final TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;
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


    private final HashMap<String, String> executorHostId = new HashMap<>();

    public GarmadonSparkListener() {
        this(SparkListernerConf.getInstance().getEventHandler(), SparkListernerConf.getInstance().getHeader());
    }

    public GarmadonSparkListener(TriConsumer<Long, Header, Object> eventHandler, Header.SerializedHeader header) {
        this.eventHandler = eventHandler;
        this.header = header;
    }


    private <T> T getValOrNull(Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return null;
        }
    }

    private void tryToSet(Runnable c) {
        try {
            c.run();
        } catch (Throwable ignored) {
        }
    }

    private void sendStageStateEvent(long stateTime, State state, String name, String stageId,
                                     String attemptId, int numTasks) {
        SparkEventProtos.StageStateEvent.Builder stageStateEventBuilder = SparkEventProtos.StageStateEvent
                .newBuilder()
                .setState(state.name())
                .setStageName(name)
                .setStageId(stageId)
                .setStageAttemptId(attemptId);

        tryToSet(() -> stageStateEventBuilder.setNumTasks(numTasks));
        this.eventHandler.accept(stateTime, header, stageStateEventBuilder.build());
    }

    private void sendExecutorStateEvent(long time, State state, String executorId, String executorHost,
                                        String reason, int taskFailures) {
        SparkEventProtos.ExecutorStateEvent.Builder executorStateEvent = SparkEventProtos.ExecutorStateEvent
                .newBuilder()
                .setState(state.name())
                .setExecutorHostname(executorHost);

        if (reason != null) {
            executorStateEvent.setReason(reason);
        }

        if (taskFailures != 0) {
            executorStateEvent.setTaskFailures(taskFailures);
        }

        this.eventHandler.accept(time, buildOverrideHeader(executorId), executorStateEvent.build());
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        try {
            header = header.cloneAndOverride(Header.newBuilder()
                    .withApplicationID(applicationStart.appId().getOrElse(emptyStringScala))
                    .withAttemptID(applicationStart.appAttemptId().getOrElse(emptyStringScala))
                    .withApplicationName(applicationStart.appName())
                    .build())
                    .toSerializeHeader();
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onApplicationStart", t);
        }
    }

    // Stage Events
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        try {
            long submissionTime = stageSubmitted.stageInfo().submissionTime().getOrElse(currentTimeLongScala);
            String name = stageSubmitted.stageInfo().name();
            String stageId = String.valueOf(stageSubmitted.stageInfo().stageId());
            String attemptId = String.valueOf(stageSubmitted.stageInfo().attemptId());
            int numTasks = stageSubmitted.stageInfo().numTasks();

            sendStageStateEvent(submissionTime, State.BEGIN, name, stageId, attemptId, numTasks);
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onStageSubmitted", t);
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        try {
            long submissionTime = stageCompleted.stageInfo().submissionTime().getOrElse(zeroLongScala);
            long completionTime = stageCompleted.stageInfo().completionTime().getOrElse(currentTimeLongScala);
            String name = stageCompleted.stageInfo().name();
            String stageId = String.valueOf(stageCompleted.stageInfo().stageId());
            String attemptId = String.valueOf(stageCompleted.stageInfo().attemptId());
            int numTasks = stageCompleted.stageInfo().numTasks();

            sendStageStateEvent(completionTime, State.END, name, stageId, attemptId, numTasks);

            String status = getValOrNull(() -> stageCompleted.stageInfo().getStatusString());

            SparkEventProtos.StageEvent.Builder stageEventBuilder = SparkEventProtos.StageEvent
                    .newBuilder()
                    .setStartTime(submissionTime)
                    .setStageName(name)
                    .setStageId(stageId)
                    .setStageAttemptId(attemptId);

            tryToSet(() -> stageEventBuilder.setNumTasks(numTasks));
            tryToSet(() -> stageEventBuilder.setStatus(status));
            tryToSet(() -> stageEventBuilder.setExecutorCpuTime(stageCompleted.stageInfo().taskMetrics().executorCpuTime()));
            tryToSet(() -> stageEventBuilder.setExecutorDeserializeCpuTime(stageCompleted.stageInfo().taskMetrics().executorDeserializeCpuTime()));
            tryToSet(() -> stageEventBuilder.setExecutorRunTime(stageCompleted.stageInfo().taskMetrics().executorRunTime()));
            tryToSet(() -> stageEventBuilder.setJvmGcTime(stageCompleted.stageInfo().taskMetrics().jvmGCTime()));
            tryToSet(() -> stageEventBuilder.setExecutorDeserializeTime(stageCompleted.stageInfo().taskMetrics().executorDeserializeTime()));
            tryToSet(() -> stageEventBuilder.setResultSerializationTime(stageCompleted.stageInfo().taskMetrics().resultSerializationTime()));
            tryToSet(() -> stageEventBuilder.setResultSize(stageCompleted.stageInfo().taskMetrics().resultSize()));
            tryToSet(() -> stageEventBuilder.setPeakExecutionMemory(stageCompleted.stageInfo().taskMetrics().peakExecutionMemory()));
            tryToSet(() -> stageEventBuilder.setDiskBytesSpilled(stageCompleted.stageInfo().taskMetrics().diskBytesSpilled()));
            tryToSet(() -> stageEventBuilder.setMemoryBytesSpilled(stageCompleted.stageInfo().taskMetrics().memoryBytesSpilled()));
            tryToSet(() -> stageEventBuilder.setShuffleReadRecords(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().recordsRead()));
            tryToSet(() -> stageEventBuilder.setShuffleReadFetchWaitTime(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().fetchWaitTime()));
            tryToSet(() -> stageEventBuilder.setShuffleReadLocalBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().localBytesRead()));
            tryToSet(() -> stageEventBuilder.setShuffleReadRemoteBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().remoteBytesRead()));
            tryToSet(() -> stageEventBuilder.setShuffleReadTotalBytes(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBytesRead()));
            tryToSet(() -> stageEventBuilder.setShuffleReadLocalBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics()
                    .localBlocksFetched()));
            tryToSet(() -> stageEventBuilder.setShuffleReadRemoteBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics()
                    .remoteBlocksFetched()));
            tryToSet(() -> stageEventBuilder.setShuffleReadTotalBlocksFetched(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics()
                    .totalBlocksFetched()));
            tryToSet(() -> stageEventBuilder.setShuffleWriteShuffleRecords(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics()
                    .shuffleRecordsWritten()));
            tryToSet(() -> stageEventBuilder.setShuffleWriteShuffleTime(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleWriteTime()));
            tryToSet(() -> stageEventBuilder.setShuffleWriteShuffleBytes(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().shuffleBytesWritten()));
            tryToSet(() -> stageEventBuilder.setInputRecords(stageCompleted.stageInfo().taskMetrics().inputMetrics().recordsRead()));
            tryToSet(() -> stageEventBuilder.setInputBytes(stageCompleted.stageInfo().taskMetrics().inputMetrics().bytesRead()));
            tryToSet(() -> stageEventBuilder.setOutputRecords(stageCompleted.stageInfo().taskMetrics().outputMetrics().recordsWritten()));
            tryToSet(() -> stageEventBuilder.setOutputBytes(stageCompleted.stageInfo().taskMetrics().outputMetrics().bytesWritten()));

            if (!"succeeded".equals(status)) {
                tryToSet(() -> stageEventBuilder.setFailureReason(stageCompleted.stageInfo().failureReason().getOrElse(emptyStringScala)));
            }

            this.eventHandler.accept(completionTime, header, stageEventBuilder.build());
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onStageCompleted", t);
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        try {
            String status = taskEnd.taskInfo().status();

            SparkEventProtos.TaskEvent.Builder taskEventBuilder = SparkEventProtos.TaskEvent
                    .newBuilder()
                    .setStartTime(taskEnd.taskInfo().launchTime())
                    .setTaskId(String.valueOf(taskEnd.taskInfo().taskId()))
                    .setStageId(String.valueOf(taskEnd.stageId()))
                    .setStageAttemptId(String.valueOf(taskEnd.stageAttemptId()))
                    .setExecutorHostname(String.valueOf(taskEnd.taskInfo().host()));

            tryToSet(() -> taskEventBuilder.setStatus(status));
            tryToSet(() -> taskEventBuilder.setLocality(taskEnd.taskInfo().taskLocality().toString()));
            tryToSet(() -> taskEventBuilder.setType(taskEnd.taskType()));
            tryToSet(() -> taskEventBuilder.setAttemptNumber(taskEnd.taskInfo().attemptNumber()));
            tryToSet(() -> taskEventBuilder.setExecutorCpuTime(taskEnd.taskMetrics().executorCpuTime()));
            tryToSet(() -> taskEventBuilder.setExecutorDeserializeCpuTime(taskEnd.taskMetrics().executorDeserializeCpuTime()));
            tryToSet(() -> taskEventBuilder.setExecutorRunTime(taskEnd.taskMetrics().executorRunTime()));
            tryToSet(() -> taskEventBuilder.setJvmGcTime(taskEnd.taskMetrics().jvmGCTime()));
            tryToSet(() -> taskEventBuilder.setExecutorDeserializeTime(taskEnd.taskMetrics().executorDeserializeTime()));
            tryToSet(() -> taskEventBuilder.setResultSerializationTime(taskEnd.taskMetrics().resultSerializationTime()));
            tryToSet(() -> taskEventBuilder.setResultSize(taskEnd.taskMetrics().resultSize()));
            tryToSet(() -> taskEventBuilder.setPeakExecutionMemory(taskEnd.taskMetrics().peakExecutionMemory()));
            tryToSet(() -> taskEventBuilder.setDiskBytesSpilled(taskEnd.taskMetrics().diskBytesSpilled()));
            tryToSet(() -> taskEventBuilder.setMemoryBytesSpilled(taskEnd.taskMetrics().memoryBytesSpilled()));
            tryToSet(() -> taskEventBuilder.setShuffleReadRecords(taskEnd.taskMetrics().shuffleReadMetrics().recordsRead()));
            tryToSet(() -> taskEventBuilder.setShuffleReadFetchWaitTime(taskEnd.taskMetrics().shuffleReadMetrics().fetchWaitTime()));
            tryToSet(() -> taskEventBuilder.setShuffleReadLocalBytes(taskEnd.taskMetrics().shuffleReadMetrics().localBytesRead()));
            tryToSet(() -> taskEventBuilder.setShuffleReadRemoteBytes(taskEnd.taskMetrics().shuffleReadMetrics().remoteBytesRead()));
            tryToSet(() -> taskEventBuilder.setShuffleReadTotalBytes(taskEnd.taskMetrics().shuffleReadMetrics().totalBytesRead()));
            tryToSet(() -> taskEventBuilder.setShuffleReadLocalBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().localBlocksFetched()));
            tryToSet(() -> taskEventBuilder.setShuffleReadRemoteBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().remoteBlocksFetched()));
            tryToSet(() -> taskEventBuilder.setShuffleReadTotalBlocksFetched(taskEnd.taskMetrics().shuffleReadMetrics().totalBlocksFetched()));
            tryToSet(() -> taskEventBuilder.setShuffleWriteShuffleRecords(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleRecordsWritten()));
            tryToSet(() -> taskEventBuilder.setShuffleWriteShuffleTime(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleWriteTime()));
            tryToSet(() -> taskEventBuilder.setShuffleWriteShuffleBytes(taskEnd.taskMetrics().shuffleWriteMetrics().shuffleBytesWritten()));
            tryToSet(() -> taskEventBuilder.setInputRecords(taskEnd.taskMetrics().inputMetrics().recordsRead()));
            tryToSet(() -> taskEventBuilder.setInputBytes(taskEnd.taskMetrics().inputMetrics().bytesRead()));
            tryToSet(() -> taskEventBuilder.setOutputRecords(taskEnd.taskMetrics().outputMetrics().recordsWritten()));
            tryToSet(() -> taskEventBuilder.setOutputBytes(taskEnd.taskMetrics().outputMetrics().bytesWritten()));

            if (!"succeeded".equals(status)) {
                tryToSet(() -> taskEventBuilder.setFailureReason(taskEnd.reason().toString()));
            }

            this.eventHandler.accept(taskEnd.taskInfo().finishTime(), buildOverrideHeader(taskEnd.taskInfo().executorId()), taskEventBuilder.build());
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onTaskEnd", t);
        }
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        try {
            executorHostId.put(executorAdded.executorId(), executorAdded.executorInfo().executorHost());
            sendExecutorStateEvent(executorAdded.time(),
                    State.ADDED,
                    executorAdded.executorId(),
                    executorAdded.executorInfo().executorHost(),
                    null,
                    0);
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onExecutorAdded", t);
        }
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        try {
            sendExecutorStateEvent(executorRemoved.time(),
                    State.REMOVED,
                    executorRemoved.executorId(),
                    executorHostId.getOrDefault(executorRemoved.executorId(), "UNKNOWN"),
                    executorRemoved.reason(),
                    0);
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onExecutorRemoved", t);
        }
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        try {
            sendExecutorStateEvent(executorBlacklisted.time(),
                    State.BLACKLISTED,
                    executorBlacklisted.executorId(),
                    executorHostId.getOrDefault(executorBlacklisted.executorId(), "UNKNOWN"),
                    null,
                    executorBlacklisted.taskFailures());
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onExecutorBlacklisted", t);
        }
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        try {
            sendExecutorStateEvent(executorUnblacklisted.time(),
                    State.UNBLACKLISTED,
                    executorUnblacklisted.executorId(),
                    executorHostId.getOrDefault(executorUnblacklisted.executorId(), "UNKNOWN"),
                    null,
                    0);
        } catch (Throwable t) {
            LOGGER.warn("Failed to send event for onExecutorUnblacklisted", t);
        }
    }

    private Header buildOverrideHeader(String executorId) {
        return header.cloneAndOverride(Header.newBuilder()
                .withExecutorId(executorId)
                .build()).toSerializeHeader();
    }
}
