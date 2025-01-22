package com.criteo.hadoop.garmadon.jvm.utils;

import junit.framework.TestCase;

import static com.criteo.hadoop.garmadon.jvm.utils.JavaRuntime.parseVersion;
import static org.assertj.core.api.Assertions.*;

public class JavaRuntimeTest extends TestCase {
    public void test_parse_1_8_x_as_8() {
        String versionString = "1.8.0_362";
        JavaRuntime.Version version = parseVersion(versionString);
        assertThat(version.version).isEqualTo(versionString);
        assertThat(version.feature).isEqualTo(8);
        assertThat(version.parsingError).isNull();
    }

    public void test_parse_11_x_as_11() {
        String versionString = "11.0.16";
        JavaRuntime.Version version = parseVersion(versionString);
        assertThat(version.version).isEqualTo(versionString);
        assertThat(version.feature).isEqualTo(11);
        assertThat(version.parsingError).isNull();
    }

    public void test_parsing_error() {
        String versionString = "ABC";
        JavaRuntime.Version version = parseVersion(versionString);
        assertThat(version.version).isEqualTo(versionString);
        assertThat(version.feature).isEqualTo(-1);
        assertThat(version.parsingError).isNotNull();
    }

    public void test_feature() {
        assertThat(JavaRuntime.feature()).isGreaterThanOrEqualTo(8);
    }

}
