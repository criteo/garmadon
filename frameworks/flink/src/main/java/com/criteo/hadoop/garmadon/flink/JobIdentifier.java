package com.criteo.hadoop.garmadon.flink;

import java.util.Objects;

public class JobIdentifier {

    private String jobId;
    private String jobName;

    public JobIdentifier(String jobId, String jobName) {
        this.jobId = jobId;
        this.jobName = jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobIdentifier that = (JobIdentifier) o;
        return Objects.equals(jobId, that.jobId) &&
            Objects.equals(jobName, that.jobName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName);
    }

    @Override
    public String toString() {
        return "JobIdentifier{" +
            "jobId='" + jobId + '\'' +
            ", jobName='" + jobName + '\'' +
            '}';
    }
}
