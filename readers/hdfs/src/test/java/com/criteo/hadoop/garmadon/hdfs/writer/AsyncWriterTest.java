package com.criteo.hadoop.garmadon.hdfs.writer;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AsyncWriterTest {

    private static ActorSystem system;

    @Before
    public void setup() {
        system = ActorSystem.create();
    }

    @After
    public void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    public static class TestRuntimeException extends RuntimeException {
    }

    @Test
    public void shouldDispatchToUnderlyingWriter() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer1 = mock(PartitionedWriter.class);
        PartitionedWriter<Object> writer2 = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", writer1);
        asyncWriter.subscribe("2", writer2);

        Instant now = Instant.now();
        CommittableOffset offset1 = mockOffset(0, 1);
        CommittableOffset offset2 = mockOffset(0, 2);
        Object msg = new Object();

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer1).write(now, offset1, msg.hashCode());

        doNothing()
            .when(writer2).write(now, offset2, msg.hashCode());

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        asyncWriter.dispatch("1", now, offset1, msg::hashCode);
        asyncWriter.dispatch("2", now, offset2, msg::hashCode);

        Thread.sleep(100);

        verify(writer1).write(now, offset1, msg.hashCode());
        verify(writer2).write(now, offset2, msg.hashCode());
        verifyCaughtNothing(caughtException);

        //test exceptions
        asyncWriter.dispatch("1", now, offset1, msg::hashCode);

        verifyCaught(TestRuntimeException.class, caughtException);
    }

    @Test
    public void shouldCloseUnderlyingWriters() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer1 = mock(PartitionedWriter.class);
        PartitionedWriter<Object> writer2 = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", writer1);
        asyncWriter.subscribe("2", writer2);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer1).close();

        doNothing()
            .doNothing()
            .when(writer2).close();

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        asyncWriter.close().get(1, TimeUnit.SECONDS);

        verify(writer1).close();
        verify(writer2).close();
        verifyCaughtNothing(caughtException);

        executeThenVerifyThrownAndCaught(asyncWriter::close, TestRuntimeException.class, caughtException);
    }

    @Test
    public void shouldHeartBeatUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer1 = mock(PartitionedWriter.class);
        PartitionedWriter<Object> writer2 = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", writer1);
        asyncWriter.subscribe("2", writer2);

        CommittableOffset offset = mockOffset(0, 1);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer1).heartbeat(1, offset);

        doNothing()
            .doNothing()
            .when(writer2).heartbeat(1, offset);

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        asyncWriter.heartbeat(1, offset);

        Thread.sleep(100);

        verify(writer1).heartbeat(1, offset);
        verify(writer2).heartbeat(1, offset);

        verifyCaughtNothing(caughtException);

        asyncWriter.heartbeat(1, offset);

        verifyCaught(TestRuntimeException.class, caughtException);
    }

    @Test
    public void shouldDropPartitionUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer1 = mock(PartitionedWriter.class);
        PartitionedWriter<Object> writer2 = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", writer1);
        asyncWriter.subscribe("2", writer2);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer1).dropPartition(1);

        doNothing()
            .doNothing()
            .when(writer2).dropPartition(1);

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        asyncWriter.dropPartition(1).get(1, TimeUnit.SECONDS);

        verify(writer1).dropPartition(1);
        verify(writer2).dropPartition(1);

        verifyCaughtNothing(caughtException);

        executeThenVerifyThrownAndCaught(() -> asyncWriter.dropPartition(1), TestRuntimeException.class, caughtException);
    }

    @Test
    public void shouldGetStartingOffsetsFromUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        PartitionedWriter<Object> firstWriter = mock(PartitionedWriter.class);
        PartitionedWriter<Object> secondWriter = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", firstWriter);
        asyncWriter.subscribe("2", secondWriter);

        final TopicPartition firstPartition = new TopicPartition("topic", 1);
        final TopicPartition secondPartition = new TopicPartition("topic", 2);
        final List<TopicPartition> partitions = Arrays.asList(firstPartition, secondPartition);

        final Map<Integer, Long> firstOffsets = new HashMap<>();
        firstOffsets.put(firstPartition.partition(), 10L);
        firstOffsets.put(secondPartition.partition(), 20L);
        when(firstWriter.getStartingOffsets(any())).thenReturn(firstOffsets);

        final Map<Integer, Long> secondOffsets = new HashMap<>();
        secondOffsets.put(firstPartition.partition(), 15L);
        secondOffsets.put(secondPartition.partition(), OffsetComputer.NO_OFFSET);
        when(secondWriter.getStartingOffsets(any())).thenReturn(secondOffsets);


        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        Collection<Integer> partitionIds = partitions
            .stream()
            .mapToInt(TopicPartition::partition)
            .boxed()
            .collect(Collectors.toList());

        Map<Integer, Long> result = asyncWriter.getStartingOffsets(partitionIds).get(1, TimeUnit.SECONDS);

        assertEquals(2, result.size());
        assertEquals(10L, result.get(firstPartition.partition()).longValue());
        assertEquals(OffsetComputer.NO_OFFSET, result.get(secondPartition.partition()).longValue());

        verifyCaughtNothing(caughtException);
    }

    @Test
    public void shouldGetStartingOffsetsFromUnderlyingWriterFailingWithIOException() throws IOException, TimeoutException, InterruptedException {
        PartitionedWriter<Object> firstWriter = mock(PartitionedWriter.class);
        PartitionedWriter<Object> secondWriter = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", firstWriter);
        asyncWriter.subscribe("2", secondWriter);

        when(firstWriter.getStartingOffsets(any())).thenReturn(new HashMap<>());
        when(secondWriter.getStartingOffsets(any())).thenThrow(new IOException());

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        try {
            asyncWriter.getStartingOffsets(Collections.singletonList(1)).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertEquals(IOException.class, e.getCause().getClass());
        }

        verifyCaughtNothing(caughtException);
        //both were called
        verify(firstWriter).getStartingOffsets(Collections.singletonList(1));
        verify(secondWriter).getStartingOffsets(Collections.singletonList(1));

    }

    @Test
    public void shouldGetStartingOffsetsFromUnderlyingWriterFailingWithUncaughtException() throws IOException, TimeoutException, InterruptedException {
        PartitionedWriter<Object> firstWriter = mock(PartitionedWriter.class);
        PartitionedWriter<Object> secondWriter = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", firstWriter);
        asyncWriter.subscribe("2", secondWriter);

        when(firstWriter.getStartingOffsets(any())).thenReturn(new HashMap<>());
        when(secondWriter.getStartingOffsets(any())).thenThrow(new TestRuntimeException());

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        try {
            asyncWriter.getStartingOffsets(Collections.singletonList(1)).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertEquals(TestRuntimeException.class, e.getCause().getClass());
        }

        verifyCaught(TestRuntimeException.class, caughtException);
        //both were called
        verify(firstWriter).getStartingOffsets(Collections.singletonList(1));
        verify(secondWriter).getStartingOffsets(Collections.singletonList(1));
    }

    @Test
    public void shouldExpireConsumersOfUnderlyingWriter() throws InterruptedException, ExecutionException, TimeoutException {
        PartitionedWriter<Object> writer1 = mock(PartitionedWriter.class);
        PartitionedWriter<Object> writer2 = mock(PartitionedWriter.class);

        AsyncWriter<Object> asyncWriter = new AsyncWriter<>();
        asyncWriter.subscribe("1", writer1);
        asyncWriter.subscribe("2", writer2);

        doNothing()
            .doThrow(new TestRuntimeException())
            .when(writer1).expireConsumers();

        doNothing()
            .doNothing()
            .when(writer2).expireConsumers();

        final Throwable[] caughtException = startAndCatchExceptions(asyncWriter);

        asyncWriter.expireConsumers();

        Thread.sleep(100);

        verify(writer1).expireConsumers();
        verify(writer2).expireConsumers();

        asyncWriter.expireConsumers();

        verifyCaught(TestRuntimeException.class, caughtException);
    }

    private void verifyCaught(Class<TestRuntimeException> exceptionClass, Throwable[] caughtException) throws InterruptedException {
        Thread.sleep(100); //Uncaught exception handler is called asynchronously, wait a bit
        assertNotNull(caughtException[0]);
        assertEquals(exceptionClass, caughtException[0].getClass());
    }

    private void verifyCaughtNothing(Throwable[] caughtException) throws InterruptedException {
        Thread.sleep(100);  //Uncaught exception handler is called asynchronously, wait a bit
        assertNull(caughtException[0]);
    }

    private <T> void executeThenVerifyThrownAndCaught(Supplier<CompletableFuture<T>> supplier, Class<TestRuntimeException> exceptionClass, Throwable[] caughtException) throws InterruptedException, TimeoutException {
        try {
            supplier.get().get(1, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            //expect exception in future
            assertEquals(exceptionClass, ex.getCause().getClass());

            Thread.sleep(100);

            verifyCaught(exceptionClass, caughtException);
            return;
        }

        fail("Expected exception to be thrown");
    }

    private Throwable[] startAndCatchExceptions(AsyncWriter<Object> asyncWriter) {
        final Throwable[] caughtException = new Throwable[1];
        asyncWriter.start(system, (t, e) -> caughtException[0] = e);
        return caughtException;
    }

    private static CommittableOffset mockOffset(long offset, int part) {
        CommittableOffset co = mock(CommittableOffset.class);
        when(co.getOffset()).thenReturn(offset);
        when(co.getPartition()).thenReturn(part);
        return co;
    }

}
