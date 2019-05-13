package com.criteo.hadoop.garmadon.schema.enums;

public enum Framework {
    YARN("YARN"),
    MAPREDUCE("MAPREDUCE"),
    SPARK("SPARK"),
    FLINK("APACHE FLINK");

    private String displayName;

    Framework(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
