package com.criteo.hadoop.garmadon.flink;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.flink.identifier.JobIdentifier;
import com.criteo.hadoop.garmadon.flink.identifier.KafkaConsumerIdentifier;
import com.criteo.hadoop.garmadon.flink.identifier.OperatorIdentifier;
import com.criteo.hadoop.garmadon.flink.identifier.TaskIdentifier;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Metric Reporter for Garmadon.
 *
 * <p>Variables in metrics scope will be sent as tags.
 */
public class GarmadonFlinkReporter implements MetricReporter, Scheduled {

    static final String HOST_VARIABLE = "<host>";
    static final String JOB_ID_VARIABLE = "<job_id>";
    static final String JOB_NAME_VARIABLE = "<job_name>";
    static final String TM_ID_VARIABLE = "<tm_id>";
    static final String TASK_ID_VARIABLE = "<task_id>";
    static final String TASK_NAME_VARIABLE = "<task_name>";
    static final String TASK_ATTEMPT_NUM_VARIABLE = "<task_attempt_num>";
    static final String SUBTASK_INDEX_VARIABLE = "<subtask_index>";
    static final String OPERATOR_ID_VARIABLE = "<operator_id>";
    static final String OPERATOR_NAME_VARIABLE = "<operator_name>";
    static final String TOPIC_VARIABLE = "<topic>";
    static final String PARTITION_VARIABLE = "<partition>";

    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonFlinkReporter.class);

    private final TriConsumer<Long, Header, Object> eventHandler;
    private final Header.SerializedHeader header;

    private Set<JobIdentifier> jobs = new HashSet<>();
    private Set<TaskIdentifier> tasks = new HashSet<>();
    private Set<OperatorIdentifier> operators = new HashSet<>();
    private Set<KafkaConsumerIdentifier> kafkaConsumers = new HashSet<>();
    private Map<String, Gauge> gauges = new HashMap<>();
    private Map<String, Counter> counters = new HashMap<>();
    private Map<String, Meter> meters = new HashMap<>();

    private Boolean isJobManager = false;
    private Boolean isTaskManager = false;


    public GarmadonFlinkReporter() {
        this(GarmadonFlinkConf.getInstance().getEventHandler(), GarmadonFlinkConf.getInstance().getHeader());
    }

    public GarmadonFlinkReporter(TriConsumer<Long, Header, Object> eventHandler, Header.SerializedHeader header) {
        this.eventHandler = eventHandler;
        this.header = header;
    }

    @Override
    public void open(MetricConfig metricConfig) {
    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {
        if (!isJobManager && !isTaskManager) {
            isJobManager = Arrays.stream(metricGroup.getScopeComponents()).anyMatch("jobmanager"::equals);
            isTaskManager = Arrays.stream(metricGroup.getScopeComponents()).anyMatch("taskmanager"::equals);
        }
        String jobId = metricGroup.getAllVariables().get(JOB_ID_VARIABLE);

        if (jobId != null) {
            String jobName = metricGroup.getAllVariables().get(JOB_NAME_VARIABLE);
            jobs.add(new JobIdentifier(jobId, jobName));

            String taskId = metricGroup.getAllVariables().get(TASK_ID_VARIABLE);
            if (taskId != null) {
                String taskName = metricGroup.getAllVariables().get(TASK_NAME_VARIABLE);
                String taskAttemptNum = metricGroup.getAllVariables().get(TASK_ATTEMPT_NUM_VARIABLE);
                String subtaskIndex = metricGroup.getAllVariables().get(SUBTASK_INDEX_VARIABLE);
                tasks.add(new TaskIdentifier(jobId, jobName, taskId, taskName, taskAttemptNum, subtaskIndex));
                String operatorId = metricGroup.getAllVariables().get(OPERATOR_ID_VARIABLE);
                if (operatorId != null) {
                    String operatorName = metricGroup.getAllVariables().get(OPERATOR_NAME_VARIABLE);
                    operators.add(new OperatorIdentifier(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName));
                    String topic = metricGroup.getAllVariables().get(TOPIC_VARIABLE);
                    if (topic != null) {
                        String partition = metricGroup.getAllVariables().get(PARTITION_VARIABLE);
                        kafkaConsumers.add(new KafkaConsumerIdentifier(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName,
                            topic, partition));
                    }
                }

            }
        }

        String metricIdentifier = metricGroup.getMetricIdentifier(s);
        String metricName = getMetricName(metricIdentifier, metricGroup);

        // There is only Gauges for now
        if (metric instanceof Counter) {
            LOGGER.info("NotifyOfAddedCounterMetric {} with name {}", metricIdentifier, metricName);
            Counter counter = (Counter) metric;
            counters.put(metricName, counter);
        } else if (metric instanceof Gauge) {
            LOGGER.info("NotifyOfAddedGaugeMetric {} with name {}", metricIdentifier, metricName);
            Gauge gauge = (Gauge) metric;
            gauges.put(metricName, gauge);
        } else if (metric instanceof Meter) {
            LOGGER.info("NotifyOfAddedMeterMetric {} with name {}", metricIdentifier, metricName);
            Meter meter = (Meter) metric;
            meters.put(metricName, meter);
        } else {
            LOGGER.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
                "does not support this metric type for metric {}.", metric.getClass().getName(), metricName);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
        String jobId = metricGroup.getAllVariables().get(JOB_ID_VARIABLE);

        if (jobId != null) {
            String jobName = metricGroup.getAllVariables().get(JOB_NAME_VARIABLE);
            jobs.remove(new JobIdentifier(jobId, jobName));

            String taskId = metricGroup.getAllVariables().get(TASK_ID_VARIABLE);
            if (taskId != null) {
                String taskName = metricGroup.getAllVariables().get(TASK_NAME_VARIABLE);
                String taskAttemptNum = metricGroup.getAllVariables().get(TASK_ATTEMPT_NUM_VARIABLE);
                String subtaskIndex = metricGroup.getAllVariables().get(SUBTASK_INDEX_VARIABLE);
                tasks.remove(new TaskIdentifier(jobId, jobName, taskId, taskName, taskAttemptNum, subtaskIndex));
                String operatorId = metricGroup.getAllVariables().get(OPERATOR_ID_VARIABLE);
                if (operatorId != null) {
                    String operatorName = metricGroup.getAllVariables().get(OPERATOR_NAME_VARIABLE);
                    operators.remove(new OperatorIdentifier(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName));
                    String topic = metricGroup.getAllVariables().get(TOPIC_VARIABLE);
                    if (topic != null) {
                        String partition = metricGroup.getAllVariables().get(PARTITION_VARIABLE);
                        kafkaConsumers.remove(new KafkaConsumerIdentifier(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName,
                            topic, partition));
                    }
                }
            }
        }

        String metricIdentifier = metricGroup.getMetricIdentifier(s);
        String metricName = getMetricName(metricIdentifier, metricGroup);

        if (metric instanceof Counter) {
            counters.remove(metricName);
        } else if (metric instanceof Gauge) {
            gauges.remove(metricName);
        } else if (metric instanceof Meter) {
            meters.remove(metricName);
        } else {
            LOGGER.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
                "does not support this metric type for metric {}.", metric.getClass().getName(), metricName);
        }
    }

    private String getMetricName(String metricIdentifier, MetricGroup metricGroup) {
        return metricIdentifier
            .replaceFirst(metricGroup.getAllVariables().get(HOST_VARIABLE) + ".", "")
            .replaceFirst("jobmanager.", "")
            .replaceFirst("nodemanager.", "")
            .replaceFirst("taskmanager.", "")
            .replaceFirst(metricGroup.getAllVariables().get(TM_ID_VARIABLE) + ".", "");
    }

    @Override
    public void report() {
        long currentTimeMillis = System.currentTimeMillis();

        if (isJobManager) {
            reportJobManagerMetrics(currentTimeMillis);
            reportJobMetrics(currentTimeMillis);
        }

        if (isTaskManager) {
            reportTaskManagerMetrics(currentTimeMillis);
            reportTaskMetrics(currentTimeMillis);
            reportOperatorMetrics(currentTimeMillis);
            reportKafkaConsumersMetrics(currentTimeMillis);
        }
    }

    private void tryToSet(Runnable c) {
        try {
            c.run();
        } catch (Throwable ignored) {
        }
    }

    private void reportJobManagerMetrics(long currentTimeMillis) {
        FlinkEventProtos.JobManagerEvent.Builder builder = FlinkEventProtos.JobManagerEvent.newBuilder();
        
        tryToSet(() -> builder.setNumRunningJobs(((Gauge<Long>) gauges.get("numRunningJobs")).getValue()));
        tryToSet(() -> builder.setNumRegisteredTaskManagers(((Gauge<Long>) gauges.get("numRegisteredTaskManagers")).getValue()));
        tryToSet(() -> builder.setTaskSlotsAvailable(((Gauge<Long>) gauges.get("taskSlotsAvailable")).getValue()));
        tryToSet(() -> builder.setTaskSlotsTotal(((Gauge<Long>) gauges.get("taskSlotsTotal")).getValue()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private void reportJobMetrics(long currentTimeMillis) {
        jobs.forEach(jobIdentifier -> reportJobMetrics(currentTimeMillis, jobIdentifier));
    }

    private void reportJobMetrics(long currentTimeMillis, JobIdentifier jobIdentifier) {
        FlinkEventProtos.JobEvent.Builder builder = FlinkEventProtos.JobEvent.newBuilder()
            .setJobId(jobIdentifier.getJobId())
            .setJobName(jobIdentifier.getJobName());

        tryToSet(() -> builder.setUptime(((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".uptime")).getValue()));
        tryToSet(() -> builder.setDowntime(((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".downtime")).getValue()));
        tryToSet(() -> builder.setRestartingTime(((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".restartingTime")).getValue()));
        tryToSet(() -> builder.setFullRestarts(((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".fullRestarts")).getValue()));
        tryToSet(() -> builder.setTotalNumberOfCheckpoints(
            ((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".totalNumberOfCheckpoints")).getValue()));
        tryToSet(() -> builder.setNumberOfInProgressCheckpoints(
            ((Gauge<Integer>) gauges.get(jobIdentifier.getJobName() + ".numberOfInProgressCheckpoints")).getValue()));
        tryToSet(() -> builder.setNumberOfFailedCheckpoints(
            ((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".numberOfFailedCheckpoints")).getValue()));
        tryToSet(() -> builder.setNumberOfCompletedCheckpoints(
            ((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".numberOfCompletedCheckpoints")).getValue()));
        tryToSet(() -> builder.setLastCheckpointRestoreTimestamp((
            (Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".lastCheckpointRestoreTimestamp")).getValue()));
        tryToSet(() -> builder.setLastCheckpointSize((
            (Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".lastCheckpointSize")).getValue()));
        tryToSet(() -> builder.setLastCheckpointDuration((
            (Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".lastCheckpointDuration")).getValue()));
        tryToSet(() -> builder.setLastCheckpointAlignmentBuffered(
            ((Gauge<Long>) gauges.get(jobIdentifier.getJobName() + ".lastCheckpointAlignmentBuffered")).getValue()));
        tryToSet(() -> builder.setLastCheckpointExternalPath(
            ((Gauge<String>) gauges.get(jobIdentifier.getJobName() + ".lastCheckpointExternalPath")).getValue()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private void reportTaskManagerMetrics(long currentTimeMillis) {
        FlinkEventProtos.TaskManagerEvent.Builder builder = FlinkEventProtos.TaskManagerEvent.newBuilder();

        tryToSet(() -> builder.setNetworkAvailableMemorySegments(
            ((Gauge<Long>) gauges.get("Status.Network.TotalMemorySegments")).getValue()));
        tryToSet(() -> builder.setNetworkAvailableMemorySegments(
            ((Gauge<Long>) gauges.get("Status.Network.AvailableMemorySegments")).getValue()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private void reportTaskMetrics(long currentTimeMillis) {
        tasks.forEach(taskIdentifier -> reportTaskMetrics(currentTimeMillis, taskIdentifier));
    }

    private void reportTaskMetrics(long currentTimeMillis, TaskIdentifier taskIdentifier) {
        FlinkEventProtos.TaskEvent.Builder builder = FlinkEventProtos.TaskEvent.newBuilder()
            .setJobId(taskIdentifier.getJobId())
            .setJobName(taskIdentifier.getJobName())
            .setTaskId(taskIdentifier.getTaskId())
            .setTaskName(taskIdentifier.getTaskName())
            .setTaskAttemptNum(taskIdentifier.getTaskAttemptNum())
            .setSubtaskIndex(taskIdentifier.getSubtaskIndex());

        tryToSet(() -> builder.setBuffersOutputQueueLength(
            ((Gauge<Long>) gauges.get(taskIdentifier.getMetricName() + "buffers.inputQueueLength")).getValue()));
        tryToSet(() -> builder.setBuffersInputQueueLength(
            ((Gauge<Long>) gauges.get(taskIdentifier.getMetricName() + "buffers.outputQueueLength")).getValue()));
        tryToSet(() -> builder.setBuffersInPoolUsage(
            ((Gauge<Long>) gauges.get(taskIdentifier.getMetricName() + "buffers.inPoolUsage")).getValue()));
        tryToSet(() -> builder.setBuffersOutPoolUsage(
            ((Gauge<Long>) gauges.get(taskIdentifier.getMetricName() + "buffers.outPoolUsage")).getValue()));
        tryToSet(() -> builder.setCurrentInputWatermark(
            ((Gauge<Long>) gauges.get(taskIdentifier.getMetricName() + "currentInputWatermark")).getValue()));

        tryToSet(() -> builder.setNumBytesOutPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBytesOutPerSecond")).getRate()));
        tryToSet(() -> builder.setNumBytesInLocalPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBytesInLocalPerSecond")).getRate()));
        tryToSet(() -> builder.setNumBytesInRemotePerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBytesInRemotePerSecond")).getRate()));

        tryToSet(() -> builder.setNumBuffersOutPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBuffersOutPerSecond")).getRate()));
        tryToSet(() -> builder.setNumBuffersInLocalPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBuffersInLocalPerSecond")).getRate()));
        tryToSet(() -> builder.setNumBuffersInRemotePerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numBuffersInRemotePerSecond")).getRate()));
        tryToSet(() -> builder.setNumRecordsOutPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numRecordsOutPerSecond")).getRate()));
        tryToSet(() -> builder.setNumRecordsOutPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numRecordsOutPerSecond")).getRate()));
        tryToSet(() -> builder.setNumRecordsInPerSecond(
            (meters.get(taskIdentifier.getMetricName() + "numRecordsInPerSecond")).getRate()));

        tryToSet(() -> builder.setNumRecordsOut(
            (counters.get(taskIdentifier.getMetricName() + "numRecordsOut")).getCount()));
        tryToSet(() -> builder.setNumRecordsIn(
            (counters.get(taskIdentifier.getMetricName() + "numRecordsIn")).getCount()));
        tryToSet(() -> builder.setNumBytesOut(
            (counters.get(taskIdentifier.getMetricName() + "numBytesOut")).getCount()));
        tryToSet(() -> builder.setNumBytesInLocal(
            (counters.get(taskIdentifier.getMetricName() + "numBytesInLocal")).getCount()));
        tryToSet(() -> builder.setNumBytesInRemote(
            (counters.get(taskIdentifier.getMetricName() + "numBytesInRemote")).getCount()));
        tryToSet(() -> builder.setNumBuffersOut(
            (counters.get(taskIdentifier.getMetricName() + "numBuffersOut")).getCount()));
        tryToSet(() -> builder.setNumBuffersInLocal(
            (counters.get(taskIdentifier.getMetricName() + "numBuffersInLocal")).getCount()));
        tryToSet(() -> builder.setNumBuffersInRemote(
            (counters.get(taskIdentifier.getMetricName() + "numBuffersInRemote")).getCount()));
        tryToSet(() -> builder.setNumLateRecordsDropped(
            (counters.get(taskIdentifier.getMetricName() + "numLateRecordsDropped")).getCount()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private void reportOperatorMetrics(long currentTimeMillis) {
        operators.forEach(operatorIdentifier -> reportOperatorMetrics(currentTimeMillis, operatorIdentifier));
    }

    private void reportOperatorMetrics(long currentTimeMillis, OperatorIdentifier operatorIdentifier) {
        FlinkEventProtos.OperatorEvent.Builder builder = FlinkEventProtos.OperatorEvent.newBuilder()
            .setJobId(operatorIdentifier.getJobId())
            .setJobName(operatorIdentifier.getJobName())
            .setTaskId(operatorIdentifier.getTaskId())
            .setTaskName(operatorIdentifier.getTaskName())
            .setSubtaskIndex(operatorIdentifier.getSubtaskIndex())
            .setOperatorId(operatorIdentifier.getOperatorId())
            .setOperatorName(operatorIdentifier.getOperatorName());

        tryToSet(() -> builder.setCurrentInputWatermark(
            ((Gauge<Long>) gauges.get(operatorIdentifier.getMetricName() + "currentInputWatermark")).getValue()));
        tryToSet(() -> builder.setCurrentInput1Watermark(
            ((Gauge<Long>) gauges.get(operatorIdentifier.getMetricName() + "currentInput1Watermark")).getValue()));
        tryToSet(() -> builder.setCurrentInput2Watermark(
            ((Gauge<Long>) gauges.get(operatorIdentifier.getMetricName() + "currentInput2Watermark")).getValue()));
        tryToSet(() -> builder.setCurrentOutputWatermark(
            ((Gauge<Long>) gauges.get(operatorIdentifier.getMetricName() + "currentOutputWatermark")).getValue()));
        tryToSet(() -> builder.setNumSplitsProcessed(
            ((Gauge<Long>) gauges.get(operatorIdentifier.getMetricName() + "numSplitsProcessed")).getValue()));

        tryToSet(() -> builder.setNumLateRecordsDropped(
            (counters.get(operatorIdentifier.getMetricName() + "numLateRecordsDropped")).getCount()));
        tryToSet(() -> builder.setCommitsSucceeded(
            (counters.get(operatorIdentifier.getMetricName() + "commitsSucceeded")).getCount()));
        tryToSet(() -> builder.setCommitsFailed(
            (counters.get(operatorIdentifier.getMetricName() + "commitsFailed")).getCount()));

        tryToSet(() -> builder.setRecordsLagMax(
            ((Gauge<Double>) gauges.get(operatorIdentifier.getMetricName() + "KafkaConsumer.records-lag-max")).getValue()));
        tryToSet(() -> builder.setRecordsConsumedRate(
            ((Gauge<Double>) gauges.get(operatorIdentifier.getMetricName() + "KafkaConsumer.records-consumed-rate")).getValue()));
        tryToSet(() -> builder.setBytesConsumedRate(
            ((Gauge<Double>) gauges.get(operatorIdentifier.getMetricName() + "KafkaConsumer.bytes-consumed-rate")).getValue()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private void reportKafkaConsumersMetrics(long currentTimeMillis) {
        kafkaConsumers.forEach(kafkaConsumerIdentifier -> reportKafkaConsumersMetrics(currentTimeMillis, kafkaConsumerIdentifier));
    }

    private void reportKafkaConsumersMetrics(long currentTimeMillis, KafkaConsumerIdentifier kafkaConsumerIdentifier) {
        FlinkEventProtos.KafkaConsumerEvent.Builder builder = FlinkEventProtos.KafkaConsumerEvent.newBuilder()
            .setJobId(kafkaConsumerIdentifier.getJobId())
            .setJobName(kafkaConsumerIdentifier.getJobName())
            .setTaskId(kafkaConsumerIdentifier.getTaskId())
            .setTaskName(kafkaConsumerIdentifier.getTaskName())
            .setSubtaskIndex(kafkaConsumerIdentifier.getSubtaskIndex())
            .setOperatorId(kafkaConsumerIdentifier.getOperatorId())
            .setOperatorName(kafkaConsumerIdentifier.getOperatorName())
            .setTopic(kafkaConsumerIdentifier.getTopic())
            .setPartition(kafkaConsumerIdentifier.getPartition());

        tryToSet(() -> builder.setCommittedOffsets(
            ((Gauge<Long>) gauges.get(kafkaConsumerIdentifier.getMetricName() + "committedOffsets")).getValue()));
        tryToSet(() -> builder.setCurrentOffsets(
            ((Gauge<Long>) gauges.get(kafkaConsumerIdentifier.getMetricName() + "currentOffsets")).getValue()));

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }
}