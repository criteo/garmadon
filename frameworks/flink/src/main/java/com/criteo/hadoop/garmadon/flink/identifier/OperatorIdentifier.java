package com.criteo.hadoop.garmadon.flink.identifier;

import java.util.Objects;

public class OperatorIdentifier {

    private String jobId;
    private String jobName;
    private String taskId;
    private String taskName;
    private String subtaskIndex;
    private String operatorId;
    private String operatorName;

    public OperatorIdentifier(String jobId, String jobName, String taskId, String taskName, String subtaskIndex,
                              String operatorId, String operatorName) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.taskId = taskId;
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.operatorId = operatorId;
        this.operatorName = operatorName;
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

    public String getMetricName() {
        return jobName + "." + operatorName + "." + subtaskIndex + ".";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperatorIdentifier that = (OperatorIdentifier) o;
        return Objects.equals(jobId, that.jobId) &&
                Objects.equals(jobName, that.jobName) &&
                Objects.equals(taskId, that.taskId) &&
                Objects.equals(taskName, that.taskName) &&
                Objects.equals(subtaskIndex, that.subtaskIndex) &&
                Objects.equals(operatorId, that.operatorId) &&
                Objects.equals(operatorName, that.operatorName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, taskId, taskName, subtaskIndex, operatorId, operatorName);
    }

    @Override
    public String toString() {
        return "OperatorIdentifier{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", taskId='" + taskId + '\'' +
                ", taskName='" + taskName + '\'' +
                ", subtaskIndex='" + subtaskIndex + '\'' +
                ", operatorId='" + operatorId + '\'' +
                ", operatorName='" + operatorName + '\'' +
                '}';
    }
}
