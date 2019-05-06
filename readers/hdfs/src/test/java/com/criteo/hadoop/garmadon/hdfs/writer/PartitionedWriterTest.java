package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.hdfs.FixedOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.TestUtils;
import com.criteo.hadoop.garmadon.hdfs.offset.Checkpointer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.function.Function;

import static com.criteo.hadoop.garmadon.hdfs.TestUtils.instantFromDate;
import static com.criteo.hadoop.garmadon.hdfs.TestUtils.localDateTimeFromDate;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class PartitionedWriterTest {
    @Test
    public void writeToMultipleDaysAndPartitions() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);
        final ExpiringConsumer<String> firstConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> thirdConsumerMock = mock(ExpiringConsumer.class);
        final int firstPartition = 11;
        final int secondPartition = 12;
        final Offset firstPartitionFirstOffset = buildOffset(firstPartition, 101);
        final Offset secondPartitionFirstOffset = buildOffset(secondPartition, 201);
        final Offset firstPartitionSecondOffset = buildOffset(firstPartition, 102);
        final Offset secondDaySecondPartitionSecondOffset = buildOffset(secondPartition, 301);

        when(writerBuilder.apply(any(LocalDateTime.class))).thenReturn(firstConsumerMock)
                .thenReturn(secondConsumerMock)
                .thenReturn(thirdConsumerMock);

        partitionedWriter.write(instantFromDate("1987-08-13 11:00:02"),
                firstPartitionFirstOffset, "D1P1M1");
        partitionedWriter.write(instantFromDate("1987-08-13 13:42:51"),
                firstPartitionSecondOffset, "D1P1M2");
        partitionedWriter.write(instantFromDate("1987-08-13 15:12:22"),
                secondPartitionFirstOffset, "D1P2M1");
        partitionedWriter.write(instantFromDate("1984-05-21 05:55:55"),
                secondDaySecondPartitionSecondOffset, "D2P2M2");

        verify(firstConsumerMock, times(1)).write(eq("D1P1M1"),
                eq(firstPartitionFirstOffset));
        verify(firstConsumerMock, times(1)).write(eq("D1P1M2"),
                eq(firstPartitionSecondOffset));
        verifyNoMoreInteractions(firstConsumerMock);

        verify(secondConsumerMock, times(1)).write(eq("D1P2M1"),
                eq(secondPartitionFirstOffset));
        verifyNoMoreInteractions(secondConsumerMock);

        verify(thirdConsumerMock, times(1)).write(eq("D2P2M2"),
                eq(secondDaySecondPartitionSecondOffset));
        verifyNoMoreInteractions(thirdConsumerMock);

        verify(writerBuilder, times(2)).apply(
                eq(localDateTimeFromDate("1987-08-13 00:00:00")));
        verify(writerBuilder, times(1)).apply(
                eq(localDateTimeFromDate("1984-05-21 00:00:00")));
        verifyNoMoreInteractions(writerBuilder);

        verifyZeroInteractions(mockCheckpointer);

        partitionedWriter.close();
        for (ExpiringConsumer<String> consumer : Arrays.asList(firstConsumerMock, secondConsumerMock, thirdConsumerMock)) {
            verify(consumer, times(1)).close();
        }

        // Highest date for each day x partition
        verify(mockCheckpointer, times(1)).tryCheckpoint(firstPartition,
                TestUtils.instantFromDate("1987-08-13 13:42:51"));
        verify(mockCheckpointer, times(1)).tryCheckpoint(secondPartition,
                TestUtils.instantFromDate("1987-08-13 15:12:22"));
        verify(mockCheckpointer, times(1)).tryCheckpoint(secondPartition,
                TestUtils.instantFromDate("1984-05-21 05:55:55"));
        verifyNoMoreInteractions(mockCheckpointer);
    }

    @Test
    public void writeOnExpired() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);
        final ExpiringConsumer<String> firstDayFirstConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> firstDaySecondConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondDayFirstConsumerMock = mock(ExpiringConsumer.class);
        final int partitionId = 11;
        final Offset firstDayFirstOffset = buildOffset(partitionId, 101);
        final Offset firstDaySecondOffset = buildOffset(partitionId, firstDayFirstOffset.getOffset() + 1);
        final Offset firstDayThirdOffset = buildOffset(partitionId, firstDaySecondOffset.getOffset() + 1);
        final Offset secondDayFirstOffset = buildOffset(partitionId, 201);
        final Offset secondDaySecondOffset = buildOffset(partitionId, secondDayFirstOffset.getOffset() + 1);

        when(writerBuilder.apply(localDateTimeFromDate("1987-08-13 00:00:00")))
                .thenReturn(firstDayFirstConsumerMock).thenReturn(firstDaySecondConsumerMock);

        when(writerBuilder.apply(localDateTimeFromDate("1984-05-21 00:00:00")))
                .thenReturn(secondDayFirstConsumerMock);

        doReturn(true).when(firstDayFirstConsumerMock).isExpired();
        doReturn(false).when(firstDaySecondConsumerMock).isExpired();
        doReturn(false).when(secondDayFirstConsumerMock).isExpired();

        partitionedWriter.write(instantFromDate("1987-08-13 05:55:55"), firstDayFirstOffset, "D1M1");
        partitionedWriter.write(instantFromDate("1984-05-21 02:22:22"), secondDayFirstOffset, "D2M1");

        // Only the first consumer should expire
        partitionedWriter.expireConsumers();

        partitionedWriter.write(instantFromDate("1987-08-13 04:44:44"), firstDaySecondOffset, "D1M2");
        partitionedWriter.write(instantFromDate("1987-08-13 03:33:33"), firstDayThirdOffset, "D1M3");
        partitionedWriter.write(instantFromDate("1984-05-21 02:22:22"), secondDaySecondOffset, "D2M2");

        verify(firstDayFirstConsumerMock, times(1)).write(eq("D1M1"), eq(firstDayFirstOffset));
        verify(firstDayFirstConsumerMock, times(1)).isExpired();
        verify(firstDayFirstConsumerMock, times(1)).close();
        verifyNoMoreInteractions(firstDayFirstConsumerMock);

        verify(firstDaySecondConsumerMock, times(1)).write(eq("D1M3"), eq(firstDayThirdOffset));
        verify(firstDaySecondConsumerMock, times(1)).write(eq("D1M2"), eq(firstDaySecondOffset));
        verifyNoMoreInteractions(firstDaySecondConsumerMock);

        verify(secondDayFirstConsumerMock, times(1)).write(eq("D2M1"), eq(secondDayFirstOffset));
        verify(secondDayFirstConsumerMock, times(1)).write(eq("D2M2"), eq(secondDaySecondOffset));

        verify(secondDayFirstConsumerMock, times(1)).isExpired();
        verifyNoMoreInteractions(secondDayFirstConsumerMock);
    }

    @Test
    public void expireNoWriter() {
        final PartitionedWriter partitionedWriter = new PartitionedWriter<String>(null,
                new FixedOffsetComputer("ignored", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mock(Checkpointer.class));

        // Should not crash
        partitionedWriter.expireConsumers();
    }

    @Test
    public void closingExceptionalConsumerThrowExceptionAfter5Retries() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
            new FixedOffsetComputer("ignored", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);
        final ExpiringConsumer<String> nonThrowingConsumer = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> throwingConsumer = mock(ExpiringConsumer.class);
        final Offset firstOffset = buildOffset(1, 101);
        final Offset secondOffset = buildOffset(1, 102);

        boolean throwException = false;

        doThrow(new IOException("Cassé")).when(throwingConsumer).close();

        when(writerBuilder.apply(eq(localDateTimeFromDate("1987-08-13 00:00:00")))).thenReturn(nonThrowingConsumer);
        when(writerBuilder.apply(eq(localDateTimeFromDate("1984-05-21 00:00:00")))).thenReturn(throwingConsumer);

        partitionedWriter.write(instantFromDate("1987-08-13 12:12:22"), firstOffset, "I don't care");
        partitionedWriter.write(instantFromDate("1984-05-21 02:22:22"), secondOffset, "Me neither");

        try {
            partitionedWriter.close();
        } catch (RuntimeException re) {
            throwException = true;
            assertEquals("Couldn't close writer for ignored", re.getMessage());
        }

        assertTrue(throwException);

        // All consumers are closed
        verify(throwingConsumer, times(5)).close();
        verify(nonThrowingConsumer, times(1)).close();
    }

    @Test
    public void checkpointFailureDoesNotFailExpiring() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final Checkpointer throwingCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), throwingCheckpointer);
        final ExpiringConsumer<String> closingConsumer = mock(ExpiringConsumer.class);
        final Offset dummyOffset = buildOffset(1, 101);

        when(writerBuilder.apply(eq(localDateTimeFromDate("1987-08-13 00:00:00")))).thenReturn(closingConsumer);
        doReturn(true).when(closingConsumer).isExpired();

        when(throwingCheckpointer.tryCheckpoint(anyInt(), any(Instant.class))).thenThrow(new RuntimeException("Logged exception"));

        partitionedWriter.write(instantFromDate("1987-08-13 12:12:22"), dummyOffset, "I don't care");
        partitionedWriter.expireConsumers();

        verify(closingConsumer, times(1)).close();

        // Even though the checkpointing mechanism threw an exception, the writer shouldn't get closed again
        partitionedWriter.expireConsumers();
        verify(closingConsumer, times(1)).close();
    }

    @Test
    public void skipMessagesBeforeLowestOffset() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 42), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);

        writer.write(Instant.EPOCH, buildOffset(1, 10), "Ignored");
        writer.write(Instant.EPOCH, buildOffset(1, 11), "Also ignored");

        verifyZeroInteractions(writerBuilder);
        verifyZeroInteractions(mockCheckpointer);
    }

    @Test
    public void dropPartition() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final OffsetComputer offsetComputer = new FixedOffsetComputer("0", 0);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder, offsetComputer,
                "ignored", DataAccessEventProtos.FsEvent.newBuilder(), mock(Checkpointer.class));
        final ExpiringConsumer<String> firstMockConsumer = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondMockConsumer = mock(ExpiringConsumer.class);
        final Instant instant = Instant.EPOCH;

        when(writerBuilder.apply(any(LocalDateTime.class))).thenReturn(firstMockConsumer).thenReturn(secondMockConsumer);

        writer.write(instant, buildOffset(1, 8), "Ignored");
        verify(firstMockConsumer, times(1)).write(anyString(), any(Offset.class));
        writer.dropPartition(1);
        writer.expireConsumers();
        writer.write(instant, buildOffset(1, 8), "Ignored");
        verify(secondMockConsumer, times(1)).write(anyString(), any(Offset.class));
        writer.expireConsumers();

        verify(secondMockConsumer, times(1)).isExpired();
        verifyNoMoreInteractions(firstMockConsumer);
        verifyNoMoreInteractions(secondMockConsumer);
    }

    @Test
    public void dropUnknownPartition() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("0", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mock(Checkpointer.class));

        when(writerBuilder.apply(any(LocalDateTime.class))).thenReturn(mock(ExpiringConsumer.class));

        // No call to #dropPartition should crash
        writer.dropPartition(123);
        writer.write(Instant.EPOCH, buildOffset(1, 8), "Ignored");
        writer.dropPartition(456);
    }

    @Test
    public void getStartingOffsets() throws IOException {
        final Map<Integer, Long> firstOffsets = new HashMap<>();
        final Map<Integer, Long> secondOffsets = new HashMap<>();
        final int firstPartition = 42;
        final int secondPartition = 51;
        final OffsetComputer offsetComputer = mock(OffsetComputer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(mock(Function.class), offsetComputer,
                "ignored", DataAccessEventProtos.FsEvent.newBuilder(), mock(Checkpointer.class));

        firstOffsets.put(firstPartition, 2L);
        firstOffsets.put(secondPartition, 3L);

        secondOffsets.put(firstPartition, 4L);
        secondOffsets.put(secondPartition, 5L);

        when(offsetComputer.computeOffsets(any())).thenReturn(firstOffsets).thenReturn(secondOffsets);

        final List<Integer> bothPartitions = Arrays.asList(firstPartition, secondPartition);
        assertEquals(firstOffsets, writer.getStartingOffsets(bothPartitions));
        assertEquals(firstOffsets, writer.getStartingOffsets(bothPartitions));

        // No unnecessary round-trip if value already filled
        verify(offsetComputer, times(1)).computeOffsets(bothPartitions);

        // If any gets dropped, then everything gets recomputed
        writer.dropPartition(firstPartition);
        assertEquals(secondOffsets, writer.getStartingOffsets(bothPartitions));
    }

    @Test
    public void heartbeatWithNoMessage() throws IOException {
        final int partition = 1;
        ArgumentCaptor<DynamicMessage> argument = ArgumentCaptor.forClass(DynamicMessage.class);
        final Offset offset = new TopicPartitionOffset("topic", partition, 123);
        final ExpiringConsumer<Message> consumer = mock(ExpiringConsumer.class);
        final OffsetComputer offsetComputer = mock(OffsetComputer.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<Message> writer = new PartitionedWriter<>((ignored) -> consumer,
                offsetComputer, "ignored", DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);

        final HashMap<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, 10L);

        when(offsetComputer.computeOffsets(any())).thenReturn(partitionOffsets);

        writer.heartbeat(partition, offset);

        verify(consumer, times(1)).write(argument.capture(), any(Offset.class));

        DynamicMessage toWrite = argument.getValue();
        String[] fields = toWrite.getAllFields().keySet().stream().map(desc -> desc.getName()).toArray(String[]::new);
        assertArrayEquals(new String[] {"timestamp", "kafka_offset"}, fields);

        verify(consumer, times(1)).close();
        verifyZeroInteractions(mockCheckpointer);
    }

    @Test
    public void heartbeatWithMessages() throws IOException {
        final int partition = 1;
        final int offsetValue = 123;
        final Offset offset = new TopicPartitionOffset("topic", partition, offsetValue);
        final ExpiringConsumer<String> consumer = mock(ExpiringConsumer.class);
        final Checkpointer mockCheckpointer = mock(Checkpointer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>((ignored) -> consumer,
                new FixedOffsetComputer("0", 0), "ignored",
                DataAccessEventProtos.FsEvent.newBuilder(), mockCheckpointer);

        writer.write(Instant.EPOCH, offset, "Message");
        writer.heartbeat(partition, offset);

        verify(consumer, never()).write(eq(null), any(Offset.class));
        verify(consumer, never()).close();

        verifyZeroInteractions(mockCheckpointer);
    }

    @Test(timeout = 3000)
    public void writerExpirer() throws InterruptedException {
        final PartitionedWriter<String> firstConsumer = mock(PartitionedWriter.class);
        final PartitionedWriter<String> secondConsumer = mock(PartitionedWriter.class);
        final PartitionedWriter.Expirer<String> expirer = new PartitionedWriter.Expirer<>(
                Arrays.asList(firstConsumer, secondConsumer), Duration.ofMillis(1));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));

        Thread.sleep(500);

        verify(firstConsumer, atLeastOnce()).expireConsumers();
        verify(secondConsumer, atLeastOnce()).expireConsumers();

        expirer.stop().join();
        verify(firstConsumer, atLeastOnce()).close();
        verify(secondConsumer, atLeastOnce()).close();
    }

    @Test(timeout = 3000)
    public void writerExpirerWithNoWriter() throws InterruptedException {
        final PartitionedWriter.Expirer<String> expirer = new PartitionedWriter.Expirer<>(Collections.emptyList(),
                Duration.ofMillis(10));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(500);
        expirer.stop().join();
    }

    @Test(timeout = 3000)
    public void writerExpirerStopWhileWaiting() throws InterruptedException {
        final PartitionedWriter.Expirer<String> expirer = new PartitionedWriter.Expirer<>(
                Collections.singleton(mock(PartitionedWriter.class)), Duration.ofHours(42));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(1000);
        expirer.stop().join();
    }

    private static Offset buildOffset(int partitionId, long offsetValue) {
        final Offset offset = mock(Offset.class);

        doReturn(partitionId).when(offset).getPartition();
        doReturn(offsetValue).when(offset).getOffset();
        doReturn("Dummy topic").when(offset).getTopic();

        return offset;
    }
}
