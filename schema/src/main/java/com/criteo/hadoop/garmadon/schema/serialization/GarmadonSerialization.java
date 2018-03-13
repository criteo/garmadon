package com.criteo.hadoop.garmadon.schema.serialization;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
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
        int METRIC_EVENT = 2;
        int STATE_EVENT = 3;
        int GC_EVENT = 1000;
        int JVMSTATS_EVENT = 1001;
        int CONTAINER_MONITORING_EVENT = 2000;
    }

    private static HashMap<Integer, Deserializer> typeMarkerToDeserializer = new HashMap<>();
    private static HashMap<Class, Serializer> classToSerializer = new HashMap<>();

    private static HashMap<Class, Integer> classToMarker = new HashMap<>();

    static {
        //hadoop events
        register(PathEvent.class, TypeMarker.PATH_EVENT, PathEvent::serialize, DataAccessEventProtos.PathEvent::parseFrom);
        register(FsEvent.class, TypeMarker.FS_EVENT, FsEvent::serialize, DataAccessEventProtos.FsEvent::parseFrom);
        register(MetricEvent.class, TypeMarker.METRIC_EVENT, MetricEvent::serialize, DataAccessEventProtos.MetricEvent::parseFrom);
        register(StateEvent.class, TypeMarker.STATE_EVENT, StateEvent::serialize, DataAccessEventProtos.StateEvent::parseFrom);

        //nodemanager events
        register(ContainerResourceEvent.class, TypeMarker.CONTAINER_MONITORING_EVENT, ContainerResourceEvent::serialize, ContainerEventProtos.ContainerResourceEvent::parseFrom);

        //jvm stats events
        register(JVMStatisticsProtos.GCStatisticsData.class, TypeMarker.GC_EVENT, JVMStatisticsProtos.GCStatisticsData::toByteArray, JVMStatisticsProtos.GCStatisticsData::parseFrom);
        register(JVMStatisticsProtos.JVMStatisticsData.class, TypeMarker.JVMSTATS_EVENT, JVMStatisticsProtos.JVMStatisticsData::toByteArray, JVMStatisticsProtos.JVMStatisticsData::parseFrom);
    }

    public static int getMarker(Class aClass) throws TypeMarkerException {
        Integer marker = classToMarker.get(aClass);
        if (marker == null) {
            throw new TypeMarkerException("can't find any type marker for class ");
        } else {
            return marker;
        }
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

    public static <T> void register(Class<T> aClass, int marker, Serializer<T> s, Deserializer d) {
        classToMarker.put(aClass, marker);
        typeMarkerToDeserializer.put(marker, d);
        classToSerializer.put(aClass, s);
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
