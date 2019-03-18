package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.event.proto.*;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Collection;

/**
 ** FIXME: These exist because ProtoParquetWriter requires a class to expose the final schema via a static getDescriptor
 ** which we cannot provide easily
 */
final public class EventsWithHeader {
    public static abstract class GCStatisticsData implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(JVMStatisticsEventsProtos.GCStatisticsData.getDescriptor());
        }
    }

    public static abstract class FsEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(DataAccessEventProtos.FsEvent.getDescriptor());
        }
    }

    public static abstract class ContainerEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(ContainerEventProtos.ContainerResourceEvent.getDescriptor());
        }
    }

    public static abstract class SparkStageEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.StageEvent.getDescriptor());
        }
    }

    public static abstract class SparkStageStateEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.StageStateEvent.getDescriptor());
        }
    }

    public static abstract class SparkExecutorStateEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.ExecutorStateEvent.getDescriptor());
        }
    }

    public static abstract class SparkTaskEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.TaskEvent.getDescriptor());
        }
    }

    public static abstract class ApplicationEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(ResourceManagerEventProtos.ApplicationEvent.getDescriptor());
        }
    }

    private static Descriptors.Descriptor descriptorForTypeWithHeader(Descriptors.Descriptor classDescriptor)
            throws Descriptors.DescriptorValidationException {
        final Collection<Descriptors.FieldDescriptor> allFields = new ArrayList<>();

        allFields.addAll(EventHeaderProtos.Header.getDescriptor().getFields());
        allFields.addAll(classDescriptor.getFields());
        DynamicMessage.Builder builder = ProtoConcatenator.buildMessageBuilder("OffsetResetter", allFields);

        return builder.getDescriptorForType();
    }
}