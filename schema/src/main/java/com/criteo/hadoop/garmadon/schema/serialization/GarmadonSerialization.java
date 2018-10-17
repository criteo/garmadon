package com.criteo.hadoop.garmadon.schema.serialization;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.*;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.jvm.JVMStatisticsProtos;

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
        int APPLICATION_EVENT = 4000;
    }

    private static HashMap<Integer, Deserializer> typeMarkerToDeserializer = new HashMap<>();
    private static HashMap<Class, Serializer> classToSerializer = new HashMap<>();

    private static HashMap<Class, Integer> classToMarker = new HashMap<>();
    private static HashMap<Integer, String> typeMarkerToName = new HashMap<>();

    static {
        // hadoop events
        register(DataAccessEventProtos.PathEvent.class, TypeMarker.PATH_EVENT, "PATH_EVENT", DataAccessEventProtos.PathEvent::toByteArray, DataAccessEventProtos.PathEvent::parseFrom);
        register(DataAccessEventProtos.FsEvent.class, TypeMarker.FS_EVENT, "FS_EVENT", DataAccessEventProtos.FsEvent::toByteArray, DataAccessEventProtos.FsEvent::parseFrom);
        register(DataAccessEventProtos.StateEvent.class, TypeMarker.STATE_EVENT, "STATE_EVENT", DataAccessEventProtos.StateEvent::toByteArray, DataAccessEventProtos.StateEvent::parseFrom);

        // nodemanager events
        register(ContainerEventProtos.ContainerResourceEvent.class, TypeMarker.CONTAINER_MONITORING_EVENT, "CONTAINER_MONITORING_EVENT", ContainerEventProtos.ContainerResourceEvent::toByteArray, ContainerEventProtos.ContainerResourceEvent::parseFrom);

        // jvm stats events
        register(JVMStatisticsProtos.GCStatisticsData.class, TypeMarker.GC_EVENT, "GC_EVENT", JVMStatisticsProtos.GCStatisticsData::toByteArray, JVMStatisticsProtos.GCStatisticsData::parseFrom);
        register(JVMStatisticsProtos.JVMStatisticsData.class, TypeMarker.JVMSTATS_EVENT, "JVMSTATS_EVENT", JVMStatisticsProtos.JVMStatisticsData::toByteArray, JVMStatisticsProtos.JVMStatisticsData::parseFrom);

        // spark events
        register(SparkEventProtos.StageEvent.class, TypeMarker.SPARK_STAGE_EVENT, "SPARK_STAGE_EVENT", SparkEventProtos.StageEvent::toByteArray, SparkEventProtos.StageEvent::parseFrom);
        register(SparkEventProtos.StageStateEvent.class, TypeMarker.SPARK_STAGE_STATE_EVENT, "SPARK_STAGE_STATE_EVENT", SparkEventProtos.StageStateEvent::toByteArray, SparkEventProtos.StageStateEvent::parseFrom);
        register(SparkEventProtos.ExecutorStateEvent.class, TypeMarker.SPARK_EXECUTOR_STATE_EVENT, "SPARK_EXECUTOR_STATE_EVENT", SparkEventProtos.ExecutorStateEvent::toByteArray, SparkEventProtos.ExecutorStateEvent::parseFrom);
        register(SparkEventProtos.TaskEvent.class, TypeMarker.SPARK_TASK_EVENT, "SPARK_TASK_EVENT", SparkEventProtos.TaskEvent::toByteArray, SparkEventProtos.TaskEvent::parseFrom);

        // resourcemanager events
        register(ResourceManagerEventProtos.ApplicationEvent.class, TypeMarker.APPLICATION_EVENT, "APPLICATION_EVENT", ResourceManagerEventProtos.ApplicationEvent::toByteArray, ResourceManagerEventProtos.ApplicationEvent::parseFrom);
    }

    public static int getMarker(Class aClass) throws TypeMarkerException {
        Integer marker = classToMarker.get(aClass);
        if (marker == null) {
            throw new TypeMarkerException("can't find any type marker for class ");
        } else {
            return marker;
        }
    }

    public static String getTypeName(int typeMarker) {
        return typeMarkerToName.getOrDefault(typeMarker, "UNKNOWN");
    }

    public static Object parseFrom(int typeMarker, InputStream is) throws DeserializationException {
        Deserializer deserializer = typeMarkerToDeserializer.get(typeMarker);
        if (deserializer == null) {
            throw new DeserializationException("there is no deserializer registered for type marker " + typeMarker);
        }
        try {
            return deserializer.deserialize(is);
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
