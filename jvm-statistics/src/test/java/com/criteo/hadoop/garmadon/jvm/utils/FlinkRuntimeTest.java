package com.criteo.hadoop.garmadon.jvm.utils;

import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkRuntimeTest extends TestCase {

    public void test_get_version() {
        assertThat(FlinkRuntime.getVersion()).isEqualTo("1.9.3");
    }

}
