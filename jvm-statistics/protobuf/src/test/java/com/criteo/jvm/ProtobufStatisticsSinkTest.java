package com.criteo.jvm;

import com.criteo.jvm.ProtobufStatisticsSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;

public class ProtobufStatisticsSinkTest {

    @Test
    public void singlePropertyOnly() {
        ProtobufStatisticsSink sink = new ProtobufStatisticsSink();
        sink.add("foo", "bar");
        Pattern expected = Pattern.compile(
                "timestamp: \\d+\n" +
                "section \\{\n" +
                "  property \\{\n" +
                "    name: \"foo\"\n" +
                "    value: \"bar\"\n" +
                "  }\n" +
                "}\n", Pattern.DOTALL);
        String actual = sink.flush().toString();
        assertThat(actual, matchesPattern(expected));
    }

    @Test
    public void emptySection() {
        ProtobufStatisticsSink sink = new ProtobufStatisticsSink();
        sink.beginSection("foo");
        sink.endSection();
        Pattern expected = Pattern.compile(
                "timestamp: \\d+\n" +
                "section \\{\n" +
                "  name: \"foo\"\n" +
                "}\n", Pattern.DOTALL);
        String actual = sink.flush().toString();
        assertThat(actual, matchesPattern(expected));
    }

    @Test
    public void sectionOneProperty() {
        ProtobufStatisticsSink sink = new ProtobufStatisticsSink();
        sink.beginSection("foo");
        sink.add("name", "value");
        sink.endSection();

        Pattern expected = Pattern.compile(
                "timestamp: \\d+\n" +
                "section \\{\n" +
                "  name: \"foo\"\n" +
                "  property \\{\n" +
                "    name: \"name\"\n" +
                "    value: \"value\"\n" +
                "  }\n" +
                "}\n", Pattern.DOTALL);
        String actual = sink.flush().toString();
        assertThat(actual, matchesPattern(expected));
    }

    @Test
    public void properties() {
        ProtobufStatisticsSink sink = new ProtobufStatisticsSink();
        sink.beginSection("section");
        sink.add("strName", "strValue");
        sink.add("intName", Integer.MAX_VALUE);
        sink.add("longName", Long.MAX_VALUE);
        sink.addDuration("durationName", 42);
        sink.addPercentage("percentageName", 100);
        sink.addSize("sizeName", 1025);
        sink.endSection();
        Pattern expected = Pattern.compile(
                "timestamp: \\d+\n" +
                "section \\{\n" +
                "  name: \"section\"\n" +
                "  property \\{\n" +
                "    name: \"strName\"\n" +
                "    value: \"strValue\"\n" +
                "  }\n" +
                "  property \\{\n" +
                "    name: \"intName\"\n" +
                "    value: \"2147483647\"\n" +
                "  }\n" +
                "  property \\{\n" +
                "    name: \"longName\"\n" +
                "    value: \"9223372036854775807\"\n" +
                "  }\n" +
                "  property \\{\n" +
                "    name: \"durationName\"\n" +
                "    value: \"42\"\n" +
                "  }\n" +
                "  property \\{\n" +
                "    name: \"%percentageName\"\n" +
                "    value: \"100\"\n" +
                "  }\n" +
                "  property \\{\n" +
                "    name: \"sizeName\"\n" +
                "    value: \"1\"\n" +
                "  }\n" +
                "}\n", Pattern.DOTALL);
        String actual = sink.flush().toString();
        assertThat(actual, matchesPattern(expected));
        // flush should have call reset
        assertThat(sink.flush().toString(), matchesPattern("timestamp: \\d+\n"));
    }
}
