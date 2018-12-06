package com.criteo.hadoop.garmadon.schema.serialization;

import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class GarmadonSerializationTest   {
    private int typeMarker = -1;


    @Test
    public void GarmadonSerialization_should_register_TestEvent() throws TypeMarkerException, DeserializationException, SerializationException {
        GarmadonSerialization.register(TestEvent.class, typeMarker, "TestEvent", TestEvent::serialize, TestEvent::parseFrom);
        assertEquals(typeMarker, GarmadonSerialization.getMarker(TestEvent.class));

        byte[] pBytes = GarmadonSerialization.serialize(new TestEvent());
        Object body = GarmadonSerialization.parseFrom(typeMarker, new ByteArrayInputStream(pBytes, 0, 2));
        assertTrue(TestEvent.class == body.getClass());

        assertEquals("TestEvent", GarmadonSerialization.getTypeName(typeMarker));
    }


}

