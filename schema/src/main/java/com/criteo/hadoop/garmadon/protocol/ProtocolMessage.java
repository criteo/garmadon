package com.criteo.hadoop.garmadon.protocol;

import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;

import java.nio.ByteBuffer;

public class ProtocolMessage {

    protected ProtocolMessage() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a garmadon protocol message
     * <p>
     * This method is useful if the application wants to save cpu
     * by serializing the header once and provide it as raw bytes
     *
     * @param hBytes
     * @param body
     * @return
     * @throws SerializationException
     * @throws TypeMarkerException
     */
    public static byte[] create(long timestamp, byte[] hBytes, Object body) throws SerializationException, TypeMarkerException {
        byte[] pBytes = GarmadonSerialization.serialize(body);
        int mark = GarmadonSerialization.getMarker(body.getClass());

        int size = ProtocolConstants.FRAME_DELIMITER_SIZE + hBytes.length + pBytes.length; //3 bytes for marker and length, then header, then body

        return ByteBuffer
                .allocate(size)
                .putInt(mark)
                .putLong(timestamp)
                .putInt(hBytes.length)
                .putInt(pBytes.length)
                .put(hBytes)
                .put(pBytes)
                .array();
    }
}
