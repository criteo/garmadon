package com.criteo.hadoop.garmadon.agent.headers;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public final class StandaloneHeaderTest {
    @Test
    public void SystemTags_are_added_to_header_tags() throws InvalidProtocolBufferException {
        List tagsInput = new ArrayList();
        tagsInput.add(Header.Tag.STANDALONE.name());
        List tagsHeader = new ArrayList();
        String tags = "garmadon,presto,compute";
        Arrays.stream(tags.split(",")).forEach(tag -> tagsInput.add(tag));

        Properties props = System.getProperties();
        props.setProperty("garmadon.tags", tags);
        Header.SerializedHeader header = StandaloneHeader.getInstance().getHeader();

        EventHeaderProtos.Header eventHeader = EventHeaderProtos.Header.parseFrom(header.serialize());
        eventHeader.getTagsList().stream().forEach(tag -> tagsHeader.add(tag));
        assertEquals(tagsInput, tagsHeader);
    }

}
