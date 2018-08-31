package com.criteo.hadoop.garmadon.elasticsearch;

import org.junit.Assert;
import org.junit.Test;

public class UriHelperTest {
    String uri = "hdfs://preprod-pa4";

    @Test
    public void getUniformizedUri_should_return_uri_as_provided() {
        Assert.assertEquals(uri, UriHelper.getUniformizedUri(uri));
    }

    @Test
    public void getUniformizedUri_should_return_uri_without_port() {
        Assert.assertEquals(uri, UriHelper.getUniformizedUri(uri + ":8020"));
    }

    @Test
    public void getUniformizedUri_should_return_uri_with_env() {
        String rootUri = "hdfs://root";
        Assert.assertEquals(uri, UriHelper.getUniformizedUri(rootUri));
    }

    @Test
    public void getUniformizedUri_should_return_uri_with_env_prefix() {
        String rootUri = "hdfs://yarn";
        Assert.assertEquals(rootUri + "-preprod-pa4", UriHelper.getUniformizedUri(rootUri));
    }
}