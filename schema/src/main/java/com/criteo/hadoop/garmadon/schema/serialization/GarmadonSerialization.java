package com.criteo.hadoop.garmadon.schema.serialization;

import com.criteo.hadoop.garmadon.event.proto.*;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Not great since we use a map to find serializer and deserializers, but at least we have everything in the same place
 * having such mapping to deserialize is necessary since we can have many events of different types to parse
 * we could later think of more performant implementations
 */
public class GarmadonSerialization {

    public interface TypeMarker {
        int PATH_EVENT = 0;
        int FS_EVENT = 1;
        int STATE_EVENT = 3;
        int GC_EVENT = 1000;
        int JVMSTATS_EVENT = 1001;
        int CONTAINER_MONITORING_EVENT = 2000;
        int SPARK_STAGE_EVENT = 3000;
        int SPARK_STAGE_STATE_EVENT = 3001;
        int SPARK_EXECUTOR_STATE_EVENT = 3002;
        int SPARK_TASK_EVENT = 3003;
        int SPARK_EXECUTOR_STORAGE_STATUS_EVENT = 3004;
        int SPARK_RDD_STORAGE_STATUS_EVENT = 3005;
        int APPLICATION_EVENT = 4000;
        int CONTAINER_EVENT = 4001;
        int FLINK_JOB_MANAGER_EVENT = 5000;
        int FLINK_JOB_EVENT = 5001;
        int FLINK_TASK_MANAGER_EVENT = 5002;
        int FLINK_TASK_EVENT = 5003;
        int FLINK_OPERATOR_EVENT = 5004;
        int FLINK_KAFKA_CONSUMER_EVENT = 5005;
    }

    private static HashMap<Integer, Deserializer> typeMarkerToDeserializer = new HashMap<>();
    private static HashMap<Class, Serializer> classToSerializer = new HashMap<>();

    private static HashMap<Class, Integer> classToMarker = new HashMap<>();
    private static HashMap<Integer, String> typeMarkerToName = new HashMap<>();
    private static HashMap<String, Integer> nameToTypeMarker = new HashMap<>();

    static {
        // hadoop events
        register(DataAccessEventProtos.PathEvent.class, TypeMarker.PATH_EVENT, "PATH_EVENT", DataAccessEventProtos.PathEvent::toByteArray,
                DataAccessEventProtos.PathEvent::parseFrom);
        register(DataAccessEventProtos.FsEvent.class, TypeMarker.FS_EVENT, "FS_EVENT", DataAccessEventProtos.FsEvent::toByteArray,
                DataAccessEventProtos.FsEvent::parseFrom);
        register(DataAccessEventProtos.StateEvent.class, TypeMarker.STATE_EVENT, "STATE_EVENT", DataAccessEventProtos.StateEvent::toByteArray,
                DataAccessEventProtos.StateEvent::parseFrom);

        // nodemanager events
        register(ContainerEventProtos.ContainerResourceEvent.class, TypeMarker.CONTAINER_MONITORING_EVENT, "CONTAINER_MONITORING_EVENT",
                ContainerEventProtos.ContainerResourceEvent::toByteArray, ContainerEventProtos.ContainerResourceEvent::parseFrom);

        // jvm stats events
        register(JVMStatisticsEventsProtos.GCStatisticsData.class, TypeMarker.GC_EVENT, "GC_EVENT", JVMStatisticsEventsProtos.GCStatisticsData::toByteArray,
                JVMStatisticsEventsProtos.GCStatisticsData::parseFrom);
        register(JVMStatisticsEventsProtos.JVMStatisticsData.class, TypeMarker.JVMSTATS_EVENT, "JVMSTATS_EVENT",
                JVMStatisticsEventsProtos.JVMStatisticsData::toByteArray, JVMStatisticsEventsProtos.JVMStatisticsData::parseFrom);

        // spark events
        register(SparkEventProtos.StageEvent.class, TypeMarker.SPARK_STAGE_EVENT, "SPARK_STAGE_EVENT", SparkEventProtos.StageEvent::toByteArray,
                SparkEventProtos.StageEvent::parseFrom);
        register(SparkEventProtos.StageStateEvent.class, TypeMarker.SPARK_STAGE_STATE_EVENT, "SPARK_STAGE_STATE_EVENT",
                SparkEventProtos.StageStateEvent::toByteArray, SparkEventProtos.StageStateEvent::parseFrom);
        register(SparkEventProtos.ExecutorStateEvent.class, TypeMarker.SPARK_EXECUTOR_STATE_EVENT, "SPARK_EXECUTOR_STATE_EVENT",
                SparkEventProtos.ExecutorStateEvent::toByteArray, SparkEventProtos.ExecutorStateEvent::parseFrom);
        register(SparkEventProtos.TaskEvent.class, TypeMarker.SPARK_TASK_EVENT, "SPARK_TASK_EVENT", SparkEventProtos.TaskEvent::toByteArray,
                SparkEventProtos.TaskEvent::parseFrom);
        register(SparkEventProtos.ExecutorStorageStatus.class, TypeMarker.SPARK_EXECUTOR_STORAGE_STATUS_EVENT, "SPARK_EXECUTOR_STORAGE_STATUS_EVENT",
            SparkEventProtos.ExecutorStorageStatus::toByteArray, SparkEventProtos.ExecutorStorageStatus::parseFrom);
        register(SparkEventProtos.RDDStorageStatus.class, TypeMarker.SPARK_RDD_STORAGE_STATUS_EVENT, "SPARK_RDD_STORAGE_STATUS_EVENT",
            SparkEventProtos.RDDStorageStatus::toByteArray, SparkEventProtos.RDDStorageStatus::parseFrom);

        // Flink events
        register(FlinkEventProtos.JobManagerEvent.class, TypeMarker.FLINK_JOB_MANAGER_EVENT, "FLINK_JOB_MANAGER_EVENT",
            FlinkEventProtos.JobManagerEvent::toByteArray, FlinkEventProtos.JobManagerEvent::parseFrom);
        register(FlinkEventProtos.JobEvent.class, TypeMarker.FLINK_JOB_EVENT, "FLINK_JOB_EVENT",
            FlinkEventProtos.JobEvent::toByteArray, FlinkEventProtos.JobEvent::parseFrom);
        register(FlinkEventProtos.TaskManagerEvent.class, TypeMarker.FLINK_TASK_MANAGER_EVENT, "FLINK_TASK_MANAGER_EVENT",
                FlinkEventProtos.TaskManagerEvent::toByteArray, FlinkEventProtos.TaskManagerEvent::parseFrom);
        register(FlinkEventProtos.TaskEvent.class, TypeMarker.FLINK_TASK_EVENT, "FLINK_TASK_EVENT",
                FlinkEventProtos.TaskEvent::toByteArray, FlinkEventProtos.TaskEvent::parseFrom);
        register(FlinkEventProtos.OperatorEvent.class, TypeMarker.FLINK_OPERATOR_EVENT, "FLINK_OPERATOR_EVENT",
                FlinkEventProtos.OperatorEvent::toByteArray, FlinkEventProtos.OperatorEvent::parseFrom);
        register(FlinkEventProtos.KafkaConsumerEvent.class, TypeMarker.FLINK_KAFKA_CONSUMER_EVENT, "FLINK_KAFKA_CONSUMER_EVENT",
            FlinkEventProtos.KafkaConsumerEvent::toByteArray, FlinkEventProtos.KafkaConsumerEvent::parseFrom);

        // resourcemanager events
        register(ResourceManagerEventProtos.ApplicationEvent.class, TypeMarker.APPLICATION_EVENT, "APPLICATION_EVENT",
                ResourceManagerEventProtos.ApplicationEvent::toByteArray, ResourceManagerEventProtos.ApplicationEvent::parseFrom);
        register(ResourceManagerEventProtos.ContainerEvent.class, TypeMarker.CONTAINER_EVENT, "CONTAINER_EVENT",
            ResourceManagerEventProtos.ContainerEvent::toByteArray, ResourceManagerEventProtos.ContainerEvent::parseFrom);
    }

    protected GarmadonSerialization() {
        throw new UnsupportedOperationException();
    }

    public static int getMarker(Class aClass) throws TypeMarkerException {
        Integer marker = classToMarker.get(aClass);
        if (marker == null) {
            throw new TypeMarkerException("can't find any type marker for class ");
        } else {
            return marker;
        }
    }

    public static int getMarker(String name) throws TypeMarkerException {
        if (nameToTypeMarker.containsKey(name)) {
            return nameToTypeMarker.get(name);
        } else {
            throw new TypeMarkerException("cannot find type marker for " + name);
        }
    }

    public static String getTypeName(int typeMarker) {
        return typeMarkerToName.getOrDefault(typeMarker, "UNKNOWN");
    }

    public static Message parseFrom(int typeMarker, InputStream is) throws DeserializationException {
        Deserializer deserializer = typeMarkerToDeserializer.get(typeMarker);
        if (deserializer == null) {
            throw new DeserializationException("there is no deserializer registered for type marker " + typeMarker);
        }
        try {
            return (Message) deserializer.deserialize(is);
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    public static byte[] serialize(Object event) throws SerializationException {
        Serializer serializer = classToSerializer.get(event.getClass());
        if (serializer == null) {
            throw new SerializationException("there is no serializer registered for type " + event.getClass());
        }
        return serializer.serialize(event);
    }

    public static <T> void register(Class<T> aClass, int marker, String name, Serializer<T> s, Deserializer d) {
        classToMarker.put(aClass, marker);
        typeMarkerToDeserializer.put(marker, d);
        classToSerializer.put(aClass, s);
        typeMarkerToName.put(marker, name);
        nameToTypeMarker.put(name, marker);
    }

    @FunctionalInterface
    public interface Deserializer {
        Object deserialize(InputStream o) throws IOException;
    }

    @FunctionalInterface
    public interface Serializer<T> {
        byte[] serialize(T o);
    }


}
