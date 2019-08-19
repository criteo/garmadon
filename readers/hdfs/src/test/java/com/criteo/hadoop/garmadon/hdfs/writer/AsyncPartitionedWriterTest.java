package com.criteo.hadoop.garmadon.hdfs.writer;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class AsyncPartitionedWriterTest {

    private static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }


    @Test
    public void shouldWriteUnderlyingWriter() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        Instant now = Instant.now();
        CommittableOffset offset = mockOffset(0, 1);
        Object msg = new Object();

        asyncPartitionedWriter.write(now, offset, msg).get(1, TimeUnit.SECONDS);

        verify(writer).write(now, offset, msg);
    }

    @Test
    public void shouldCloseUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        asyncPartitionedWriter.close().get(1, TimeUnit.SECONDS);

        verify(writer).close();
    }

    @Test
    public void shouldHeartBeatUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        CommittableOffset offset = mockOffset(0, 1);

        asyncPartitionedWriter.heartbeat(1, offset).get(1, TimeUnit.SECONDS);

        verify(writer).heartbeat(1, offset);
    }

    @Test
    public void shouldDropPartitionUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        asyncPartitionedWriter.dropPartition(1).get(1, TimeUnit.SECONDS);

        verify(writer).dropPartition(1);
    }

    @Test
    public void shouldGetStartingOffsetsFromUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        Collection<Integer> partitions = Collections.singleton(2);
        Map<Integer, Long> startingOffsets = Collections.singletonMap(1, 10L);
        when(writer.getStartingOffsets(partitions)).thenReturn(startingOffsets);

        Map<Integer, Long> result = asyncPartitionedWriter.getStartingOffsets(partitions).get(1, TimeUnit.SECONDS);

        verify(writer).getStartingOffsets(partitions);
        assertEquals(startingOffsets, result);
    }

    @Test
    public void shouldExpireConsumersOfUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        asyncPartitionedWriter.expireConsumers().get(1, TimeUnit.SECONDS);

        verify(writer).expireConsumers();
    }

    private static CommittableOffset mockOffset(long offset, int part) {
        CommittableOffset co = mock(CommittableOffset.class);
        when(co.getOffset()).thenReturn(offset);
        when(co.getPartition()).thenReturn(part);
        return co;
    }
}
