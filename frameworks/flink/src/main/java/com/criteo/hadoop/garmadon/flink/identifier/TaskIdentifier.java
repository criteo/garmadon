package com.criteo.hadoop.garmadon.flink.identifier;

import java.util.Objects;

public class TaskIdentifier {

    private String jobId;
    private String jobName;
    private String taskId;
    private String taskName;
    private String taskAttemptNum;
    private String subtaskIndex;

    public TaskIdentifier(String jobId, String jobName, String taskId, String taskName, String taskAttemptNum, String subtaskIndex) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.taskId = taskId;
        this.taskName = taskName;
        this.taskAttemptNum = taskAttemptNum;
        this.subtaskIndex = subtaskIndex;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getTaskAttemptNum() {
        return taskAttemptNum;
    }

    public String getSubtaskIndex() {
        return subtaskIndex;
    }

    public String getJobName() {
        return jobName;
    }

    public String getMetricName() {
        return jobName + "." + taskName + "." + subtaskIndex + ".";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskIdentifier that = (TaskIdentifier) o;
        return Objects.equals(jobId, that.jobId) &&
                Objects.equals(jobName, that.jobName) &&
                Objects.equals(taskId, that.taskId) &&
                Objects.equals(taskName, that.taskName) &&
                Objects.equals(taskAttemptNum, that.taskAttemptNum) &&
                Objects.equals(subtaskIndex, that.subtaskIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, taskId, taskName, taskAttemptNum, subtaskIndex);
    }

    @Override
    public String toString() {
        return "TaskIdentifier{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", taskId='" + taskId + '\'' +
                ", taskName='" + taskName + '\'' +
                ", taskAttemptNum='" + taskAttemptNum + '\'' +
                ", subtaskIndex='" + subtaskIndex + '\'' +
                '}';
    }
}
