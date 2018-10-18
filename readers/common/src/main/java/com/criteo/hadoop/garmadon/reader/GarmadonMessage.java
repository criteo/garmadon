package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;

/**
 * A message from GarmadonReader
 *
 * It contains the type of the event, as a 'magic' int
 * the header, allowing to retrieve information like appId, user, etc...
 * the body which contains the unmarshalled object.
 *
 * Since GarmadonReader can produce messages of different types,
 * it is necessary to cast the object to the good type.
 *
 * Finally, GarmadonMessage carries a CommittableOffset object
 * that allows the reader application to commit the kafka offset
 * whenever it wants.
 *
 * It is recommended not to commit all offsets to kafka for performance reasons
 */
public class GarmadonMessage {
    private final int type;
    private final EventHeaderProtos.Header header;
    private final Object body;
    private final CommittableOffset committableOffset;

    public GarmadonMessage(int type, EventHeaderProtos.Header header, Object body, CommittableOffset committableOffset) {
        this.type = type;
        this.header = header;
        this.body = body;
        this.committableOffset = committableOffset;
    }

    public int getType() {
        return type;
    }

    public EventHeaderProtos.Header getHeader() {
        return header;
    }

    public Object getBody() {
        return body;
    }

    public CommittableOffset getCommittableOffset() {
        return committableOffset;
    }
}
