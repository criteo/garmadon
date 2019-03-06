package com.criteo.hadoop.garmadon.flink.identifier;

import java.util.Objects;

public class KafkaConsumerIdentifier {

    private String jobId;
    private String jobName;
    private String taskId;
    private String taskName;
    private String subtaskIndex;
    private String operatorId;
    private String operatorName;
    private String topic;
    private String partition;

    public KafkaConsumerIdentifier(String jobId, String jobName, String taskId, String taskName, String subtaskIndex,
                                   String operatorId, String operatorName, String topic, String partition) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.taskId = taskId;
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.operatorId = operatorId;
        this.operatorName = operatorName;
        this.topic = topic;
        this.partition = partition;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getSubtaskIndex() {
        return subtaskIndex;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }

    public String getMetricName() {
        return jobName + "." + operatorName + "." + subtaskIndex + ".KafkaConsumer.topic." + topic + ".partition." + partition + ".";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaConsumerIdentifier that = (KafkaConsumerIdentifier) o;
        return Objects.equals(jobId, that.jobId) &&
            Objects.equals(jobName, that.jobName) &&
            Objects.equals(taskId, that.taskId) &&
            Objects.equals(taskName, that.taskName) &&
            Objects.equals(subtaskIndex, that.subtaskIndex) &&
            Objects.equals(operatorId, that.operatorId) &&
            Objects.equals(operatorName, that.operatorName) &&
            Objects.equals(topic, that.topic) &&
            Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName, topic, partition);
    }

    @Override
    public String toString() {
        return "KafkaConsumerIdentifier{" +
            "jobId='" + jobId + '\'' +
            ", jobName='" + jobName + '\'' +
            ", taskId='" + taskId + '\'' +
            ", taskName='" + taskName + '\'' +
            ", subtaskIndex='" + subtaskIndex + '\'' +
            ", operatorId='" + operatorId + '\'' +
            ", operatorName='" + operatorName + '\'' +
            ", topic='" + topic + '\'' +
            ", partition='" + partition + '\'' +
            '}';
    }
}
