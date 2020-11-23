package com.criteo.hadoop.garmadon.hdfs.hive;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.stream.Collectors;

public class HiveClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveClient.class);

    private final HiveQueryExecutor executor;
    private final Set<Pair<String, String>> createdPartitions;
    private final Set<String> createdTables = new HashSet<>();

    public HiveClient(HiveQueryExecutor executor, String location) throws SQLException {
        this.createdPartitions = Collections.newSetFromMap(new LinkedHashMap<Pair<String, String>, Boolean>() {
            protected boolean removeEldestEntry(Entry<Pair<String, String>, Boolean> eldest) {
                return size() > 100;
            }
        });
        this.executor = executor;
        // TODO discover mesos slave to contact
        this.executor.connect();
        this.executor.createDatabaseIfAbsent(location);
    }

    public void createPartitionIfNotExist(String table, MessageType schema, String partition, String location) throws SQLException {
        if (partitionAlreadyCreated(partition, table)) {
            LOGGER.info("Partition day={} on {} already exists, skipping creation", partition, table);
            return;
        }

        createTableIfNotExist(table, schema, location);

        this.executor.createPartition(table, partition, location);

        registerPartitionCreation(partition, table);
    }

    @VisibleForTesting
    public void createTableIfNotExist(String table, MessageType schema, String location) throws SQLException {
        if (tableAlreadyCreated(table)) {
            LOGGER.info("Table {} already exists, skipping creation", table);
            return;
        }

        String hiveSchema = schema.getFields().stream().map(field -> {
            try {
                return field.getName() + " " + inferHiveType(field);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.joining(", "));

        this.executor.createTableIfNotExists(table, hiveSchema, location);

        registerTableCreation(table);
    }

    @VisibleForTesting
    public String inferHiveType(Type field) throws Exception {
        String fieldHiveType;

        switch (field.asPrimitiveType().getPrimitiveTypeName().name()) {
            case "BINARY":
                fieldHiveType = "string";
                break;
            case "INT32":
                fieldHiveType = "int";
                break;
            case "INT64":
                fieldHiveType = "bigint";
                break;
            case "FLOAT":
                fieldHiveType = "float";
                break;
            case "DOUBLE":
                fieldHiveType = "double";
                break;
            case "BOOLEAN":
                fieldHiveType = "boolean";
                break;
            default:
                throw new Exception("Unsupported Data Type: " + field.asPrimitiveType().getPrimitiveTypeName().name());
        }

        if (field.isRepetition(Type.Repetition.REPEATED)) {
            fieldHiveType = "array<" + fieldHiveType + ">";
        }

        return fieldHiveType;
    }

    public void close() throws SQLException {
        executor.close();
    }

    private boolean partitionAlreadyCreated(String partition, String table) {
        return createdPartitions.contains(Pair.of(partition, table));
    }

    private void registerPartitionCreation(String partition, String table) {
        createdPartitions.add(Pair.of(partition, table));
    }

    private boolean tableAlreadyCreated(String table) {
        return createdTables.contains(table);
    }

    private void registerTableCreation(String table) {
        createdTables.add(table);
    }
}
