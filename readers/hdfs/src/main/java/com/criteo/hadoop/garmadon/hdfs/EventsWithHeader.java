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

    public static abstract class ContainerResourceEvent implements Message {
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

    public static abstract class SparkRddStorageStatus implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.RDDStorageStatus.getDescriptor());
        }
    }

    public static abstract class SparkExecutorStorageStatus implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(SparkEventProtos.ExecutorStorageStatus.getDescriptor());
        }
    }

    public static abstract class ApplicationEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(ResourceManagerEventProtos.ApplicationEvent.getDescriptor());
        }
    }

    public static abstract class ContainerEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(ResourceManagerEventProtos.ContainerEvent.getDescriptor());
        }
    }

    public static abstract class FlinkJobManagerEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.JobManagerEvent.getDescriptor());
        }
    }

    public static abstract class FlinkJobEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.JobEvent.getDescriptor());
        }
    }

    public static abstract class FlinkTaskManagerEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.TaskManagerEvent.getDescriptor());
        }
    }

    public static abstract class FlinkTaskEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.TaskEvent.getDescriptor());
        }
    }

    public static abstract class FlinkOperatorEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.OperatorEvent.getDescriptor());
        }
    }

    public static abstract class FlinkKafkaConsumerEvent implements Message {
        public static Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
            return descriptorForTypeWithHeader(FlinkEventProtos.KafkaConsumerEvent.getDescriptor());
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