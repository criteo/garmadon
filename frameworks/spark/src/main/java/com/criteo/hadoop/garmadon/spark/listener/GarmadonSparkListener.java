package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.State;
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

    private void sendStageStateEvent(long completionTime, State state, String name, String stageId,
                                     String attemptId, int numTasks) {
        DataAccessEventProtos.StateEvent stateEvent = DataAccessEventProtos.StateEvent
                .newBuilder()
                .setTimestamp(completionTime)
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
}
