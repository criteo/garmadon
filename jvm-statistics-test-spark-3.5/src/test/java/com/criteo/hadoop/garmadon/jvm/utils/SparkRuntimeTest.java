package com.criteo.hadoop.garmadon.jvm.utils;

import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkRuntimeTest extends TestCase {

    public void test_get_3_5_version() {
        assertThat(SparkRuntime.getVersion()).isEqualTo("3.5.3");
    }

}
