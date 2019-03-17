package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Arrays;
import java.util.Map;

/**
 * A message from GarmadonReader
 * <p>
 * It contains the type of the event, as a 'magic' int
 * the header, allowing to retrieve information like appId, user, etc...
 * the body which contains the unmarshalled object.
 * <p>
 * Since GarmadonReader can produce messages of different types,
 * it is necessary to cast the object to the good type.
 * <p>
 * Finally, GarmadonMessage carries a CommittableOffset object
 * that allows the reader application to commit the kafka offset
 * whenever it wants.
 * <p>
 * It is recommended not to commit all offsets to kafka for performance reasons
 */
public class GarmadonMessage {
    private final int type;

    // timestamp in millisecond
    private final long timestamp;
    private final EventHeaderProtos.Header header;
    private final Message body;
    private final CommittableOffset committableOffset;

    public GarmadonMessage(int type, long timestamp, EventHeaderProtos.Header header, Message body, CommittableOffset committableOffset) {
        this.type = type;
        this.timestamp = timestamp;
        this.header = header;
        this.body = body;
        this.committableOffset = committableOffset;
    }

    public int getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EventHeaderProtos.Header getHeader() {
        return header;
    }

    public Message getBody() {
        return body;
    }

    public CommittableOffset getCommittableOffset() {
        return committableOffset;
    }

    public Map<String, Object> getHeaderMap(boolean includeDefaultValue) {
        return ProtoConcatenator.concatToMap(timestamp, Arrays.asList(header), includeDefaultValue);
    }

    public Map<String, Object> toMap(boolean includeDefaultValue, boolean addTypeMarker) {
        Map<String, Object> eventMap = ProtoConcatenator.concatToMap(timestamp, Arrays.asList(header, body), includeDefaultValue);

        if (addTypeMarker) {
            eventMap.put("event_type", GarmadonSerialization.getTypeName(type));
        }

        // Specific normalization for FS_EVENT
        if (GarmadonSerialization.TypeMarker.FS_EVENT == type) {
            String uri = (String) eventMap.get("uri");
            eventMap.computeIfPresent("src_path", (k, v) -> ((String) v).replace(uri, ""));
            eventMap.computeIfPresent("dst_path", (k, v) -> ((String) v).replace(uri, ""));
            eventMap.computeIfPresent("uri", (k, v) -> UriHelper.getUniformizedUri((String) v));
        }

        return eventMap;
    }

    public Message toProto() {
        Message.Builder messageBuilder = ProtoConcatenator.concatToProtobuf(timestamp, committableOffset.getOffset(), Arrays.asList(header, body));

        // Specific normalization for FS_EVENT
        if (GarmadonSerialization.TypeMarker.FS_EVENT == type) {
            Descriptors.FieldDescriptor uriFD = messageBuilder.getDescriptorForType().findFieldByName("uri");
            Descriptors.FieldDescriptor srcPathFD = messageBuilder.getDescriptorForType().findFieldByName("src_path");
            Descriptors.FieldDescriptor dstPathFD = messageBuilder.getDescriptorForType().findFieldByName("dst_path");

            if (uriFD != null && srcPathFD != null && dstPathFD != null) {
                String uri = (String) messageBuilder.getField(uriFD);
                String srcPath = (String) messageBuilder.getField(srcPathFD);
                String dstPath = (String) messageBuilder.getField(dstPathFD);

                srcPath = srcPath.replace(uri, "");
                dstPath = dstPath.replace(uri, "");
                uri = UriHelper.getUniformizedUri(uri);

                if (!srcPath.isEmpty()) {
                    messageBuilder.setField(srcPathFD, srcPath);
                }

                messageBuilder.setField(dstPathFD, dstPath);
                messageBuilder.setField(uriFD, uri);
            }
        }

        return messageBuilder.build();
    }
}
