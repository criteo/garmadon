package com.criteo.hadoop.garmadon.jvm.utils;

import junit.framework.TestCase;

import static com.criteo.hadoop.garmadon.jvm.utils.JavaRuntime.parseVersion;
import static org.assertj.core.api.Assertions.*;

public class JavaRuntimeTest extends TestCase {
    public void test_parse_1_8_x_as_8() {
        JavaRuntime.Version version = parseVersion("1.8.0_362");
        assertThat(version.getVersionNumber()).isEqualTo(8);
        assertThat(version.getParsingError()).isNull();
    }

    public void test_parse_11_x_as_11() {
        JavaRuntime.Version version = parseVersion("11.0.16");
        assertThat(version.getVersionNumber()).isEqualTo(11);
        assertThat(version.getParsingError()).isNull();
    }

    public void test_parsing_error() {
        JavaRuntime.Version version = parseVersion("ABC");
        assertThat(version.getVersionNumber()).isEqualTo(-1);
        assertThat(version.getParsingError()).isNotNull();
    }

    public void test_getVersion() {
        assertThat(JavaRuntime.getVersion()).isGreaterThanOrEqualTo(8);
    }

}
