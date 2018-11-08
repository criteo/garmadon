package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.immutable.HashMap;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class GarmadonSparkListenerTest {
    private static final Header DUMMY_HEADER = new Header("id", "appId", "appAttemptId",
            "appName", "user", "container", "hostname",
            Collections.singletonList("tag"), "pid", "framework", "component",
            "executorId", "mainClass");

    @Test
    public void onExecutorAdded() {
        TriConsumer<Long, Header, Object> handlerMock = mock(TriConsumer.class);
        SparkListener listener = new GarmadonSparkListener(handlerMock, DUMMY_HEADER.toSerializeHeader());

        listener.onExecutorAdded(new SparkListenerExecutorAdded(1, "invokationExecutorId",
                new ExecutorInfo("infoHost", 12, new HashMap<>())));
        checkExecutorEventHeader(handlerMock, DUMMY_HEADER.cloneAndOverride(Header.newBuilder()
                .withExecutorId("invokationExecutorId").build()));
    }

    @Test
    public void onExecutorRemoved() {
        TriConsumer<Long,Header, Object> handlerMock = mock(TriConsumer.class);
        SparkListener listener = new GarmadonSparkListener(handlerMock, DUMMY_HEADER.toSerializeHeader());

        listener.onExecutorRemoved(new SparkListenerExecutorRemoved(1, "invokationExecutorId",
                "reason"));
        checkExecutorEventHeader(handlerMock, DUMMY_HEADER.cloneAndOverride(Header.newBuilder()
                .withExecutorId("invokationExecutorId").build()));
    }

    @Test
    public void onExecutorBlacklisted() {
        TriConsumer<Long,Header, Object> handlerMock = mock(TriConsumer.class);
        SparkListener listener = new GarmadonSparkListener(handlerMock, DUMMY_HEADER.toSerializeHeader());

        listener.onExecutorBlacklisted(new SparkListenerExecutorBlacklisted(1, "invokationExecutorId",
                12));
        checkExecutorEventHeader(handlerMock, DUMMY_HEADER.cloneAndOverride(Header.newBuilder()
                .withExecutorId("invokationExecutorId").build()));
    }

    @Test
    public void onExecutorUnblacklisted() {
        TriConsumer<Long,Header, Object> handlerMock = mock(TriConsumer.class);
        SparkListener listener = new GarmadonSparkListener(handlerMock, DUMMY_HEADER.toSerializeHeader());

        listener.onExecutorUnblacklisted(new SparkListenerExecutorUnblacklisted(1, "invokationExecutorId"));
        checkExecutorEventHeader(handlerMock, DUMMY_HEADER.cloneAndOverride(Header.newBuilder()
                .withExecutorId("invokationExecutorId").build()));
    }

    private void checkExecutorEventHeader(TriConsumer<Long,Header, Object> handlerMock, Header expectedHeader) {
        ArgumentCaptor<Header> argumentCaptor = ArgumentCaptor.forClass(Header.class);
        verify(handlerMock, times(1)).accept(any(Long.class), argumentCaptor.capture(), any(Object.class));
        Assert.assertEquals(expectedHeader, argumentCaptor.getValue());
    }
}
