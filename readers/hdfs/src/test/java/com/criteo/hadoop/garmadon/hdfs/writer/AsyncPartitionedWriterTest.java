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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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

    public static class TestRuntimeException extends RuntimeException {
    }

    @Test
    public void shouldWriteUnderlyingWriter() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        Instant now = Instant.now();
        CommittableOffset offset = mockOffset(0, 1);
        Object msg = new Object();

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer).write(now, offset, msg.hashCode());

        asyncPartitionedWriter.write(now, offset, msg::hashCode).get(1, TimeUnit.SECONDS);

        verify(writer).write(now, offset, msg.hashCode());

        expectExecutionException(() -> asyncPartitionedWriter.write(now, offset, msg::hashCode));
    }

    @Test
    public void shouldCloseUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer).close();

        asyncPartitionedWriter.close().get(1, TimeUnit.SECONDS);

        verify(writer).close();

        expectExecutionException(asyncPartitionedWriter::close);
    }

    @Test
    public void shouldHeartBeatUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        CommittableOffset offset = mockOffset(0, 1);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer).heartbeat(1, offset);

        asyncPartitionedWriter.heartbeat(1, offset).get(1, TimeUnit.SECONDS);

        verify(writer).heartbeat(1, offset);

        expectExecutionException(() -> asyncPartitionedWriter.heartbeat(1, offset));
    }

    @Test
    public void shouldDropPartitionUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer).dropPartition(1);

        asyncPartitionedWriter.dropPartition(1).get(1, TimeUnit.SECONDS);

        verify(writer).dropPartition(1);

        expectExecutionException(() -> asyncPartitionedWriter.dropPartition(1));
    }

    @Test
    public void shouldGetStartingOffsetsFromUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        Collection<Integer> partitions = Collections.singleton(2);
        Map<Integer, Long> startingOffsets = Collections.singletonMap(1, 10L);

        doReturn(startingOffsets)
            .doThrow(new TestRuntimeException())
            .when(writer).getStartingOffsets(partitions);

        Map<Integer, Long> result = asyncPartitionedWriter.getStartingOffsets(partitions).get(1, TimeUnit.SECONDS);

        verify(writer).getStartingOffsets(partitions);
        assertEquals(startingOffsets, result);

        expectExecutionException(() -> asyncPartitionedWriter.getStartingOffsets(partitions));
    }

    @Test
    public void shouldExpireConsumersOfUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer = mock(PartitionedWriter.class);
        AsyncPartitionedWriter<Object> asyncPartitionedWriter = AsyncPartitionedWriter.create(system, writer);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer).expireConsumers();

        asyncPartitionedWriter.expireConsumers().get(1, TimeUnit.SECONDS);

        verify(writer).expireConsumers();

        expectExecutionException(asyncPartitionedWriter::expireConsumers);
    }

    private static CommittableOffset mockOffset(long offset, int part) {
        CommittableOffset co = mock(CommittableOffset.class);
        when(co.getOffset()).thenReturn(offset);
        when(co.getPartition()).thenReturn(part);
        return co;
    }

    private static <T> void expectExecutionException(Supplier<CompletableFuture<T>> supplier) {
        try {
            supplier.get().get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return;
        } catch (Exception e) {
            fail("expected an ExecutionException. Got " + e.getClass());
        }
        fail("expected to receive an exception");
    }
}
