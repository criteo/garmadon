package com.criteo.hadoop.garmadon.hdfs.configurations;

public class HiveConfiguration {
    private boolean createHiveTable = false;
    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String hiveJdbcUrl;
    private String hiveDatabase = "garmadon";

    public boolean isCreateHiveTable() {
        return createHiveTable;
    }

    public void setCreateHiveTable(boolean createHiveTable) {
        this.createHiveTable = createHiveTable;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getHiveJdbcUrl() {
        return hiveJdbcUrl;
    }

    public void setHiveJdbcUrl(String hiveJdbcUrl) {
        this.hiveJdbcUrl = hiveJdbcUrl;
    }

    public String getHiveDatabase() {
        return hiveDatabase;
    }

    public void setHiveDatabase(String hiveDatabase) {
        this.hiveDatabase = hiveDatabase;
    }
}
