package com.criteo.hadoop.garmadon.forwarder.handler.junit.rules;

import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.rules.ExternalResource;

public class WithEmbeddedChannel extends ExternalResource {

    private EmbeddedChannel channel;

    @Override
    protected void before() throws Throwable {
        channel = new EmbeddedChannel();
    }

    @Override
    protected void after() {
        channel.close();
    }

    public EmbeddedChannel get(){
        return channel;
    }

}
