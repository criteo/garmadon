package com.criteo.hadoop.garmadon.heuristics.configurations;

public class DbConfiguration {
    private String connectionString;
    private String user;
    private String password;
    private long sleepRetry = 15000L;

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getSleepRetry() {
        return sleepRetry;
    }

    public void setSleepRetry(long sleepRetry) {
        this.sleepRetry = sleepRetry;
    }
}
