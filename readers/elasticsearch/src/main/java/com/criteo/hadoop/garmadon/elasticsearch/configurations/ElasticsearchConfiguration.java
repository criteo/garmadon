package com.criteo.hadoop.garmadon.elasticsearch.configurations;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchConfiguration {
    private String host;
    private int port;
    private String scheme = "http";
    private String user;
    private String password;
    private String indexPrefix = "garmadon";
    private int bulkConcurrent = 10;
    private int bulkActions = 500;
    private int bulkSizeMB = 5;
    private int bulkFlushIntervalSec = 10;
    private boolean ilmForceMerge = false;
    private int ilmTimingDayForWarmPhase;
    private int ilmTimingDayForDeletePhase;
    private Map<String, String> settings = new HashMap<>();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
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

    public String getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(String indexPrefix) {
        this.indexPrefix = indexPrefix;
    }

    public int getBulkConcurrent() {
        return bulkConcurrent;
    }

    public void setBulkConcurrent(int bulkConcurrent) {
        this.bulkConcurrent = bulkConcurrent;
    }

    public int getBulkActions() {
        return bulkActions;
    }

    public void setBulkActions(int bulkActions) {
        this.bulkActions = bulkActions;
    }

    public int getBulkSizeMB() {
        return bulkSizeMB;
    }

    public void setBulkSizeMB(int bulkSizeMB) {
        this.bulkSizeMB = bulkSizeMB;
    }

    public int getBulkFlushIntervalSec() {
        return bulkFlushIntervalSec;
    }

    public void setBulkFlushIntervalSec(int bulkFlushIntervalSec) {
        this.bulkFlushIntervalSec = bulkFlushIntervalSec;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    public boolean isIlmForceMerge() {
        return ilmForceMerge;
    }

    public void setIlmForceMerge(boolean ilmForceMerge) {
        this.ilmForceMerge = ilmForceMerge;
    }

    public int getIlmTimingDayForWarmPhase() {
        return ilmTimingDayForWarmPhase;
    }

    public void setIlmTimingDayForWarmPhase(int ilmTimingDayForWarmPhase) {
        this.ilmTimingDayForWarmPhase = ilmTimingDayForWarmPhase;
    }

    public int getIlmTimingDayForDeletePhase() {
        return ilmTimingDayForDeletePhase;
    }

    public void setIlmTimingDayForDeletePhase(int ilmTimingDayForDeletePhase) {
        this.ilmTimingDayForDeletePhase = ilmTimingDayForDeletePhase;
    }
}
