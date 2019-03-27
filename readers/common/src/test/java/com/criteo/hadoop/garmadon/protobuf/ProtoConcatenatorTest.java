package com.criteo.hadoop.garmadon.protobuf;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ProtoConcatenatorTest {
    @Test
    public void concatenateEmptyList() {
        long kafkaOffset = 0L;
        HashMap<String, Object> expectedValues = new HashMap<>();
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Collections.emptyList(), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateSingleMessage() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 21L;
        DynamicMessage.Builder inMsg = createBodyBuilder();
        Descriptors.Descriptor inMsgDesc = inMsg.getDescriptorForType();

        inMsg.setField(inMsgDesc.findFieldByName("bodyInt"), 1);
        inMsg.setField(inMsgDesc.findFieldByName("bodyString"), "one");

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("bodyInt", 1);
        expectedValues.put("bodyString", "one");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 21L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(21L, Collections.singletonList(inMsg.build()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessageWithEmptyValue() throws Descriptors.DescriptorValidationException {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        MessageDefinition msgDef = MessageDefinition.newBuilder("Body")
                .addField("optional", "int32", "emptyint32", 1)
                .addField("optional", "string", "emptystring", 2)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("emptyint32", 0);
        expectedValues.put("emptystring", "");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);

        testToMapOutTypesWith(0L, Collections.singletonList(schema.newMessageBuilder("Body").build()), expectedValues);
    }

    @Test
    public void concatenateDifferentMessages() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage.Builder headerMessageBuilder = createHeaderMessageBuilder();
        Descriptors.Descriptor headerMsgDesc = headerMessageBuilder.getDescriptorForType();

        headerMessageBuilder.setField(headerMsgDesc.findFieldByName("id"), 1)
                .setField(headerMsgDesc.findFieldByName("name"), "one");

        DynamicMessage.Builder bodyMessageBuilder = createBodyBuilder();
        Descriptors.Descriptor bodyMsgDesc = bodyMessageBuilder.getDescriptorForType();

        bodyMessageBuilder.setField(bodyMsgDesc.findFieldByName("bodyInt"), 2)
                .setField(bodyMsgDesc.findFieldByName("bodyString"), "two");

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("id", 1);
        expectedValues.put("name", "one");
        expectedValues.put("bodyInt", 2);
        expectedValues.put("bodyString", "two");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(headerMessageBuilder.build(), bodyMessageBuilder.build()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessageWithItself() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage.Builder headerMessageBuilder = createHeaderMessageBuilder();
        Descriptors.Descriptor headerMsgDesc = headerMessageBuilder.getDescriptorForType();

        headerMessageBuilder.setField(headerMsgDesc.findFieldByName("id"), 1)
                .setField(headerMsgDesc.findFieldByName("name"), "one");

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("id", 1);
        expectedValues.put("name", "one");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        // As of proto3, two fields cannot have the same name (even if they have a different id)
        Assert.assertNull(ProtoConcatenator.concatToProtobuf(0, 0,
                Arrays.asList(headerMessageBuilder.build(), headerMessageBuilder.build())));
    }

    @Test
    public void concatenateMessageWithEmpty() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage.Builder headerMessageBuilder = createHeaderMessageBuilder();
        Descriptors.Descriptor headerMsgDesc = headerMessageBuilder.getDescriptorForType();

        headerMessageBuilder.setField(headerMsgDesc.findFieldByName("id"), 1)
                .setField(headerMsgDesc.findFieldByName("name"), "one");

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("id", 1);
        expectedValues.put("name", "one");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(headerMessageBuilder.build(), createEmptyMessage()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateEmptyWithEmpty() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        HashMap<String, Object> expectedValues = new HashMap<>();
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(createEmptyMessage(), createEmptyMessage()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessagesWithClashingNamesAndIds() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage.Builder headerMessageBuilder = createHeaderMessageBuilder();
        Descriptors.Descriptor headerMsgDesc = headerMessageBuilder.getDescriptorForType();

        headerMessageBuilder.setField(headerMsgDesc.findFieldByName("id"), 1)
                .setField(headerMsgDesc.findFieldByName("name"), "one");

        DynamicMessage.Builder bodyMessageBuilder = createOtherHeaderMessageBuilder();
        Descriptors.Descriptor bodyMsgDesc = bodyMessageBuilder.getDescriptorForType();

        bodyMessageBuilder.setField(bodyMsgDesc.findFieldByName("otherId"), 2)
                .setField(bodyMsgDesc.findFieldByName("otherName"), "two");

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("id", 1);
        expectedValues.put("name", "one");
        expectedValues.put("otherId", 2);
        expectedValues.put("otherName", "two");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(headerMessageBuilder.build(), bodyMessageBuilder.build()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessageWithAllTypesWithEmpty() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage allTypesMessage = createMessageWithAllProtoTypes();

        Map<String, Object> expectedValues = new HashMap<>();
        for (int i = 0; i < ALL_PROTOBUF_TYPES.size(); i++) {
            expectedValues.put(ALL_PROTOBUF_TYPES.get(i), ALL_PROTOBUF_DEFAULT_VALUES.get(i));
        }
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(allTypesMessage, createEmptyMessage()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessageWithRepeatedFieldWithEmpty() throws Descriptors.DescriptorValidationException {
        long kafkaOffset = 0L;
        DynamicMessage.Builder messageWithRepeatedFieldBuilder = createMessageWithRepeatedField();
        Descriptors.Descriptor msgDesc = messageWithRepeatedFieldBuilder.getDescriptorForType();

        Message msg = messageWithRepeatedFieldBuilder
                .addRepeatedField(msgDesc.findFieldByName("repeated_field"), 1)
                .addRepeatedField(msgDesc.findFieldByName("repeated_field"), 2)
                .build();

        Map<String, Object> expectedValues = new HashMap<>();
        ArrayList<Object> ints = new ArrayList<>(Arrays.asList(1, 2));
        expectedValues.put("repeated_field", ints);
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, kafkaOffset);

        testAllOutTypesWith(0L, Arrays.asList(msg, createEmptyMessage()), expectedValues, kafkaOffset);
    }

    @Test
    public void concatenateMessageWithEnumAndRegularFields() throws Descriptors.DescriptorValidationException {
        DynamicMessage.Builder messageWithEnumBuilder = createMessageWithEnumsAndRegularFields();
        Descriptors.Descriptor msgDesc = messageWithEnumBuilder.getDescriptorForType();

        Message msg = messageWithEnumBuilder
                .setField(msgDesc.findFieldByName("the_enum_1"), msgDesc.findEnumTypeByName("Enum1").findValueByName("TEST_1"))
                .setField(msgDesc.findFieldByName("regular_field"), "value")
                .build();

        Map<String, Object> expectedValues = new HashMap<>();
        expectedValues.put("regular_field", "value");
        expectedValues.put("the_enum_1", "TEST_1");
        expectedValues.put("the_enum_2", "DEFAULT");
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.put(ProtoConcatenator.KAFKA_OFFSET, 0L);

        testAllOutTypesWith(0L, Collections.singletonList(msg), expectedValues, 0L);
    }

    /**
     * Test input with all ProtoConcatenator methods/flavors
     *
     * @param inputMessages  Input Protobuf messages
     * @param expectedValues Strictly-expected values (must be equal in size and values to the output)
     */
    private void testAllOutTypesWith(long timestampMillis, Collection<Message> inputMessages, Map<String, Object> expectedValues, long kafkaOffset) {
        Message outProtoMessage = ProtoConcatenator.concatToProtobuf(timestampMillis, kafkaOffset, inputMessages).build();

        Assert.assertNotNull(outProtoMessage);
        Assert.assertEquals(expectedValues.size(), outProtoMessage.getDescriptorForType().getFields().size());
        for (Map.Entry<String, Object> v : expectedValues.entrySet()) {
            Assert.assertEquals(v.getValue(), getProtoFieldValueByName(outProtoMessage, v.getKey()));
        }
        expectedValues.put(ProtoConcatenator.TIMESTAMP_FIELD_NAME, 0L);
        expectedValues.remove(ProtoConcatenator.KAFKA_OFFSET);

        testToMapOutTypesWith(0L, inputMessages, expectedValues);
    }

    /**
     * Test input with all ProtoConcatenator methods/flavors
     *
     * @param inputMessages  Input Protobuf messages
     * @param expectedValues Strictly-expected values (must be equal in size and values to the output)
     */
    private void testToMapOutTypesWith(long timestampMillis, Collection<Message> inputMessages, Map<String, Object> expectedValues) {
        Map<String, Object> outMap = ProtoConcatenator.concatToMap(timestampMillis, inputMessages, true);

        Assert.assertNotNull(outMap);
        Assert.assertEquals(expectedValues.size(), outMap.size());
        for (Map.Entry<String, Object> v : expectedValues.entrySet()) {
            Assert.assertEquals(v.getValue(), outMap.get(v.getKey()));
        }
    }

    private static Object getProtoFieldValueByName(Message message, String fieldName) {
        Object field = message.getField(message.getDescriptorForType().findFieldByName(fieldName));
        if(field instanceof Descriptors.EnumValueDescriptor){
            return ((Descriptors.EnumValueDescriptor) field).getName();
        } else {
            return field;
        }
    }

    private static final List<Object> ALL_PROTOBUF_DEFAULT_VALUES = Arrays.asList(0d, 0f, 0, 0L, 0, 0L, 0, 0L, 0, 0L, 0,
            0L, true, "", ByteString.EMPTY);
    private static final List<String> ALL_PROTOBUF_TYPES = Arrays.asList("double", "float", "int32", "int64", "uint32",
            "uint64", "sint32", "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64", "bool", "string", "bytes");

    private static DynamicMessage createEmptyMessage() throws Descriptors.DescriptorValidationException {
        String messageName = "Empty";
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();

        builder.addMessageDefinition(MessageDefinition.newBuilder(messageName).build());

        return builder.build().newMessageBuilder(messageName).build();
    }

    // Create a header with two fields: id and name
    private static DynamicMessage.Builder createHeaderMessageBuilder() throws Descriptors.DescriptorValidationException {
        String messageName = "Header";
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

        MessageDefinition msgDef = MessageDefinition.newBuilder(messageName)
                .addField("required", "int32", "id", 1)
                .addField("optional", "string", "name", 2)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder(messageName);
    }

    // Create a message with all Protobuf types
    private static DynamicMessage createMessageWithAllProtoTypes() throws Descriptors.DescriptorValidationException {
        String messageName = "AllTypes";
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        MessageDefinition.Builder msgBuilder = MessageDefinition.newBuilder(messageName);

        int currentIndex = 1;
        for (String typeName : ALL_PROTOBUF_TYPES) {
            msgBuilder.addField("required", typeName, typeName, currentIndex++);
        }

        schemaBuilder.addMessageDefinition(msgBuilder.build());
        DynamicSchema schema = schemaBuilder.build();
        DynamicMessage.Builder builder = schema.newMessageBuilder(messageName);

        Descriptors.Descriptor msgDesc = builder.getDescriptorForType();
        for (int i = 0; i < ALL_PROTOBUF_DEFAULT_VALUES.size(); ++i) {
            builder.setField(msgDesc.findFieldByNumber(i + 1), ALL_PROTOBUF_DEFAULT_VALUES.get(i));
        }

        return builder.build();
    }

    // Create a header with identical indexes & types as Header, but different names
    private static DynamicMessage.Builder createOtherHeaderMessageBuilder() throws Descriptors.DescriptorValidationException {
        String messageName = "OtherHeader";
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

        MessageDefinition msgDef = MessageDefinition.newBuilder(messageName)
                .addField("required", "int32", "otherId", 1)
                .addField("optional", "string", "otherName", 2)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder(messageName);
    }

    // Create a body with indexes and field names different from Header & OtherHeader
    private static DynamicMessage.Builder createBodyBuilder() throws Descriptors.DescriptorValidationException {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Body")
                .addField("required", "int32", "bodyInt", 3)
                .addField("optional", "string", "bodyString", 4)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder("Body");
    }

    private static DynamicMessage.Builder createMessageWithRepeatedField()
            throws Descriptors.DescriptorValidationException {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Repeated")
                .addField("repeated", "int32", "repeated_field", 1)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder("Repeated");
    }

    private static DynamicMessage.Builder createMessageWithEnumsAndRegularFields() throws Descriptors.DescriptorValidationException {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

        EnumDefinition enumDefinition_1 = EnumDefinition
                .newBuilder("Enum1")
                .addValue("DEFAULT", 0)
                .addValue("TEST_1", 1)
                .build();

        EnumDefinition enumDefinition_2 = EnumDefinition
                .newBuilder("Enum2")
                .addValue("DEFAULT", 0)
                .addValue("TEST_2", 1)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("EnumsAndRegularFields")
                .addEnumDefinition(enumDefinition_1)
                .addEnumDefinition(enumDefinition_2)
                .addField("required", "Enum1", "the_enum_1", 1)
                .addField("optional", "Enum2", "the_enum_2", 2)
                .addField("required", "string", "regular_field", 3)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        return schema.newMessageBuilder("EnumsAndRegularFields");
    }
}
