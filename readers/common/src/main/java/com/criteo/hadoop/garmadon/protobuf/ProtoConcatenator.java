package com.criteo.hadoop.garmadon.protobuf;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ProtoConcatenator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoConcatenator.class);

    protected ProtoConcatenator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Concatenate Protobuf messages into a single Protobuf message.
     * /!\ Doesn't handle embedded objects /!\
     *
     * @param messages Messages to be concatenated
     * @return A single, one-level Protobuf objects holding fields and values from all input messages.
     * Null if an error occurred (shouldn't happen).
     */
    public static Message concatToProtobuf(Collection<Message> messages) {
        try {
            final DynamicMessage.Builder messageBuilder = concatInner(messages,
                    (keys) -> {
                        try {
                            return buildMessageBuilder("GeneratedObject", keys);
                        } catch (Descriptors.DescriptorValidationException e) {
                            LOGGER.error("Couldn't build concatenated Protobuf", e);
                            throw new IllegalArgumentException(e);
                        }
                    },
                    (entry, builder) -> {
                        String fieldName = entry.getKey().getName();
                        Descriptors.Descriptor descriptorForType = builder.getDescriptorForType();
                        Descriptors.FieldDescriptor dstFieldDescriptor = descriptorForType.findFieldByName(fieldName);

                        if (dstFieldDescriptor == null) {
                            throw new IllegalArgumentException("Tried to fill a non-existing field: " + fieldName);
                        }

                        if (dstFieldDescriptor.isRepeated()) {
                            setRepeatedField(builder, dstFieldDescriptor, entry);
                        } else {
                            builder.setField(dstFieldDescriptor, entry.getValue());
                        }
                    });

            return messageBuilder.build();
        } catch (IllegalArgumentException e) {
            LOGGER.error("Could not flatten Protobuf event", e);
            return null;
        }
    }

    /**
     * Concatenate Protobuf messages into a single (String, Object) map.
     * /!\ Doesn't handle embedded objects /!\
     *
     * @param messages Messages to be concatenated
     * @param includeDefaultValueFields Boolean indicating if empty fields must be added with their default
     * @return A single, one-level (String, Object) map holding fields and values from all input messages.
     * Null if an error occurred (shouldn't happen).
     */
    public static Map<String, Object> concatToMap(Collection<Message> messages, boolean includeDefaultValueFields) {
        return concatInner(messages,
                (keys) -> {
                    Map<String, Object> concatMap = new HashMap<>(keys.size());
                    if (includeDefaultValueFields) {
                        for (Descriptors.FieldDescriptor fieldDescriptor : keys) {
                            concatMap.put(fieldDescriptor.getName(), fieldDescriptor.getDefaultValue());
                        }
                    }
                    return concatMap;
                },
                (entry, eventMap) -> {
                    eventMap.put(entry.getKey().getName(), entry.getValue());
                });
    }

    /**
     * Build a dynamic message builder based on a list of fields. Fields will be numbered in the order they were
     * provided, starting from 1
     *
     * @param msgName Name of the output message
     * @param fields  Fields to be added to the output message definition
     * @return A builder able to fill a message made of all input fields
     * @throws Descriptors.DescriptorValidationException In case of a bug (shouldn't happen)
     */
    public static DynamicMessage.Builder buildMessageBuilder(String msgName, Collection<Descriptors.FieldDescriptor> fields)
            throws Descriptors.DescriptorValidationException {
        final MessageDefinition.Builder msgDef = MessageDefinition.newBuilder(msgName);

        int currentIndex = 1;

        for (Descriptors.FieldDescriptor fieldDescriptor : fields) {
            String label;

            if (fieldDescriptor.isRepeated()) {
                label = "repeated";
            } else label = (fieldDescriptor.isRequired()) ? "required" : "optional";

            msgDef.addField(label,
                    fieldDescriptor.getType().toString().toLowerCase(), fieldDescriptor.getName(), currentIndex++);
        }

        final DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.addMessageDefinition(msgDef.build());

        final DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder(msgName);
    }

    /**
     * Concatenate Protobuf messages and feeds them to parameterizable consumers
     * /!\ Doesn't handle embedded objects and repeated fields /!\
     *
     * @param messages         Messages to be concatenated
     * @param messageBuilder   Callback invoked with all input fields. Returns the object to be filled by
     *                         contentsConsumer.
     * @param contentsConsumer Callback invoked for every output field + contents, along with the messageBuilder.
     *                         Returns false if something wrong happened.
     * @return Whether all operations completed successfully
     */
    private static <MESSAGE_TYPE> MESSAGE_TYPE concatInner(Collection<Message> messages,
                                                           Function<Collection<Descriptors.FieldDescriptor>, MESSAGE_TYPE> messageBuilder,
                                                           BiConsumer<Map.Entry<Descriptors.FieldDescriptor, Object>, MESSAGE_TYPE> contentsConsumer) {
        final Collection<Map.Entry<Descriptors.FieldDescriptor, Object>> allFields = new HashSet<>();
        for (Message message : messages) {
            allFields.addAll(message.getAllFields().entrySet());
        }

        final Collection<Descriptors.FieldDescriptor> allKeys = new ArrayList<>();
        for (Message message : messages) {
            allKeys.addAll(message.getDescriptorForType().getFields());
        }

        final MESSAGE_TYPE msg = messageBuilder.apply(allKeys);
        allFields.forEach(field -> contentsConsumer.accept(field, msg));

        return msg;
    }

    private static void setRepeatedField(DynamicMessage.Builder builder, Descriptors.FieldDescriptor dstFieldDescriptor,
                                         Map.Entry<Descriptors.FieldDescriptor, Object> entry) {
        @SuppressWarnings("unchecked")
        final Collection<Object> values = (Collection<Object>) entry.getValue();

        for (Object value : values) {
            builder.addRepeatedField(dstFieldDescriptor, value);
        }
    }
}
