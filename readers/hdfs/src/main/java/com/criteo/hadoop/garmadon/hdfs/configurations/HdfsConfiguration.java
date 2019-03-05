package com.criteo.hadoop.garmadon.hdfs.configurations;

public class HdfsConfiguration {
    private String baseTemporaryDir;
    private String finalDir;
    private int messagesBeforeExpiringWriters = 3_000_000;
    private int writersExpirationDelay = 30;
    private int expirerPeriod = 30;
    private int heartbeatPeriod = 320;
    private int maxTmpFileOpenRetries = 10;
    private int tmpFileOpenRetryPeriod = 30;
    private int sizeBeforeFlushingTmp = 16;
    private int backlogDays = 2;

    public String getBaseTemporaryDir() {
        return baseTemporaryDir;
    }

    public void setBaseTemporaryDir(String baseTemporaryDir) {
        this.baseTemporaryDir = baseTemporaryDir;
    }

    public String getFinalDir() {
        return finalDir;
    }

    public void setFinalDir(String finalHdfsDir) {
        this.finalDir = finalHdfsDir;
    }

    public int getMessagesBeforeExpiringWriters() {
        return messagesBeforeExpiringWriters;
    }

    public void setMessagesBeforeExpiringWriters(int messagesBeforeExpiringWriters) {
        this.messagesBeforeExpiringWriters = messagesBeforeExpiringWriters;
    }

    public int getWritersExpirationDelay() {
        return writersExpirationDelay;
    }

    public void setWritersExpirationDelay(int writersExpirationDelay) {
        this.writersExpirationDelay = writersExpirationDelay;
    }

    public int getExpirerPeriod() {
        return expirerPeriod;
    }

    public void setExpirerPeriod(int expirerPeriod) {
        this.expirerPeriod = expirerPeriod;
    }

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public void setHeartbeatPeriod(int heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public int getMaxTmpFileOpenRetries() {
        return maxTmpFileOpenRetries;
    }

    public void setMaxTmpFileOpenRetries(int maxTmpFileOpenRetries) {
        this.maxTmpFileOpenRetries = maxTmpFileOpenRetries;
    }

    public int getTmpFileOpenRetryPeriod() {
        return tmpFileOpenRetryPeriod;
    }

    public void setTmpFileOpenRetryPeriod(int tmpFileOpenRetryPeriod) {
        this.tmpFileOpenRetryPeriod = tmpFileOpenRetryPeriod;
    }

    public int getSizeBeforeFlushingTmp() {
        return sizeBeforeFlushingTmp;
    }

    public void setSizeBeforeFlushingTmp(int sizeBeforeFlushingTmp) {
        this.sizeBeforeFlushingTmp = sizeBeforeFlushingTmp;
    }

    public int getBacklogDays() {
        return backlogDays;
    }

    public void setBacklogDays(int backlogDays) {
        this.backlogDays = backlogDays;
    }
}
