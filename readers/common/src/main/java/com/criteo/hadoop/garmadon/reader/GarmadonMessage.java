package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.google.protobuf.Message;

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

    GarmadonMessage(int type, long timestamp, EventHeaderProtos.Header header, Message body, CommittableOffset committableOffset) {
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
}
