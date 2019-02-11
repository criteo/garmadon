package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class StandaloneHeaderTest {
    private List tagsInput;
    private List tagsHeader;

    @Before
    public void setUp() {
        tagsInput = new ArrayList();
        tagsInput.add(Header.Tag.STANDALONE.name());
        tagsHeader = new ArrayList();
    }

    private void headerTags(String tags) throws InvalidProtocolBufferException {
        Arrays.stream(tags.split(",")).forEach(tag -> tagsInput.add(tag.toUpperCase()));
        Properties props = System.getProperties();
        props.setProperty("garmadon.tags", tags);
        Header.SerializedHeader header = StandaloneHeader.getInstance().getHeader();

        EventHeaderProtos.Header eventHeader = EventHeaderProtos.Header.parseFrom(header.serialize());
        eventHeader.getTagsList().stream().forEach(tag -> tagsHeader.add(tag));
    }

    @Test
    public void SystemTags_are_added_to_header_tags() throws InvalidProtocolBufferException {
        String tags = "garma-don,pres.to,compute,test_test";
        headerTags(tags);
        assertEquals(tagsInput, tagsHeader);
    }

    @Test
    public void SystemTags_with_bad_char_are_not_added_to_header_tags() throws InvalidProtocolBufferException {
        String tags = "garm*don";
        headerTags(tags);
        assertTrue(!tagsHeader.contains(tags.toUpperCase()));
    }

}
