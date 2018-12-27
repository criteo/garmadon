package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.FixedOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static com.criteo.hadoop.garmadon.hdfs.TestUtils.instantFromDate;
import static com.criteo.hadoop.garmadon.hdfs.TestUtils.localDateTimeFromDate;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class PartitionedWriterTest {
    @Test
    public void writeToMultipleDaysAndPartitions() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored");
        final ExpiringConsumer<String> firstConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> thirdConsumerMock = mock(ExpiringConsumer.class);
        final Offset firstPartitionFirstOffset = buildOffset(11, 101);
        final Offset secondPartitionFirstOffset = buildOffset(12, 201);
        final Offset firstPartitionSecondOffset = buildOffset(11, 102);
        final Offset secondDaysecondPartitionSecondOffset = buildOffset(12, 301);

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
                secondDaysecondPartitionSecondOffset, "D2P2M2");

        verify(firstConsumerMock, times(1)).write(eq("D1P1M1"),
                eq(firstPartitionFirstOffset));
        verify(firstConsumerMock, times(1)).write(eq("D1P1M2"),
                eq(firstPartitionSecondOffset));
        verifyNoMoreInteractions(firstConsumerMock);

        verify(secondConsumerMock, times(1)).write(eq("D1P2M1"),
                eq(secondPartitionFirstOffset));
        verifyNoMoreInteractions(secondConsumerMock);

        verify(thirdConsumerMock, times(1)).write(eq("D2P2M2"),
                eq(secondDaysecondPartitionSecondOffset));
        verifyNoMoreInteractions(thirdConsumerMock);

        verify(writerBuilder, times(2)).apply(
                eq(localDateTimeFromDate("1987-08-13 00:00:00")));
        verify(writerBuilder, times(1)).apply(
                eq(localDateTimeFromDate("1984-05-21 00:00:00")));
        verifyNoMoreInteractions(writerBuilder);

        partitionedWriter.close();
        for (ExpiringConsumer<String> consumer: Arrays.asList(firstConsumerMock, secondConsumerMock, thirdConsumerMock)) {
            verify(consumer, times(1)).close();
        }
    }

    @Test
    public void writeOnExpired() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored");
        final ExpiringConsumer<String> firstDayFirstConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> firstDaySecondConsumerMock = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondDayFirstConsumerMock = mock(ExpiringConsumer.class);
        final int partitionId = 11;
        final Offset firstDayFirstOffset = buildOffset(partitionId, 101);
        final Offset firstDaySecondOffset = buildOffset(partitionId,firstDayFirstOffset.getOffset() + 1);
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
                new FixedOffsetComputer("ignored", 0), "ignored");

        // Should not crash
        partitionedWriter.expireConsumers();
    }

    @Test
    public void closingExceptionalConsumerDoesNotThrow() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> partitionedWriter = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 0), "ignored");
        final ExpiringConsumer<String> nonThrowingConsumer = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> throwingConsumer = mock(ExpiringConsumer.class);
        final Offset firstOffset = buildOffset(1, 101);
        final Offset secondOffset = buildOffset(1, 102);

        doThrow(new IOException("Cassé")).when(throwingConsumer).close();

        when(writerBuilder.apply(eq(localDateTimeFromDate("1987-08-13 00:00:00")))).thenReturn(nonThrowingConsumer);
        when(writerBuilder.apply(eq(localDateTimeFromDate("1984-05-21 00:00:00")))).thenReturn(throwingConsumer);

        partitionedWriter.write(instantFromDate("1987-08-13 12:12:22"), firstOffset, "I don't care");
        partitionedWriter.write(instantFromDate("1984-05-21 02:22:22"), secondOffset, "Me neither");

        // Should not throw
        partitionedWriter.close();

        // All consumers are closed
        verify(throwingConsumer, times(1)).close();
        verify(nonThrowingConsumer, times(1)).close();
    }

    @Test
    public void skipMessagesBeforeLowestOffset() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder,
                new FixedOffsetComputer("ignored", 42), "ignored");

        writer.write(Instant.EPOCH, buildOffset(1, 10), "Ignored");
        writer.write(Instant.EPOCH, buildOffset(1, 11), "Also ignored");

        verifyZeroInteractions(writerBuilder);
    }

    @Test
    public void dropPartition() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final OffsetComputer offsetComputer = mock(OffsetComputer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder, offsetComputer, "ignored");
        final ExpiringConsumer<String> firstMockConsumer = mock(ExpiringConsumer.class);
        final ExpiringConsumer<String> secondMockConsumer = mock(ExpiringConsumer.class);
        final Instant instant = Instant.EPOCH;

        when(offsetComputer.computeOffset(anyInt())).thenReturn(0L);
        when(writerBuilder.apply(any(LocalDateTime.class))).thenReturn(firstMockConsumer).thenReturn(secondMockConsumer);

        writer.write(instant, buildOffset(1, 8), "Ignored");
        verify(firstMockConsumer, times(1)).write(anyString(), any(Offset.class));
        writer.dropPartition(1);
        writer.expireConsumers();
        writer.write(instant, buildOffset(1, 8), "Ignored");
        verify(secondMockConsumer, times(1)).write(anyString(), any(Offset.class));
        writer.expireConsumers();

        verify(secondMockConsumer, times(1)).isExpired();
        verify(offsetComputer, times(2)).computeOffset(anyInt());
        verifyNoMoreInteractions(firstMockConsumer);
        verifyNoMoreInteractions(secondMockConsumer);
    }

    @Test
    public void dropUnknownPartition() throws IOException {
        final Function<LocalDateTime, ExpiringConsumer<String>> writerBuilder = mock(Function.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(writerBuilder, mock(OffsetComputer.class), "ignored");

        when(writerBuilder.apply(any(LocalDateTime.class))).thenReturn(mock(ExpiringConsumer.class));

        // No call to #dropPartition should crash
        writer.dropPartition(123);
        writer.write(Instant.EPOCH, buildOffset(1, 8), "Ignored");
        writer.dropPartition(456);
    }

    @Test
    public void getStartingOffset() throws IOException {
        final long firstOffset = 12;
        final long secondOffset = 30;
        final int partition = 42;
        final OffsetComputer offsetComputer = mock(OffsetComputer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>(mock(Function.class), offsetComputer,
                "ignored");

        when(offsetComputer.computeOffset(anyInt())).thenReturn(firstOffset).thenReturn(secondOffset);

        Assert.assertEquals(firstOffset, writer.getStartingOffset(partition));
        Assert.assertEquals(firstOffset, writer.getStartingOffset(partition));

        // No unnecessary round-trip if value already filled
        verify(offsetComputer, times(1)).computeOffset(partition);

        writer.dropPartition(partition);
        Assert.assertEquals(secondOffset, writer.getStartingOffset(partition));
    }

    @Test
    public void heartbeatWithNoMessage() throws IOException {
        final int partition = 1;
        final Offset offset = new TopicPartitionOffset("topic", partition, 123);
        final ExpiringConsumer<String> consumer = mock(ExpiringConsumer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>((ignored) -> consumer,
                mock(OffsetComputer.class), "ignored");

        writer.heartbeat(partition, offset);

        verify(consumer, times(1)).write(eq(null), any(Offset.class));
        verify(consumer, times(1)).close();
    }

    @Test
    public void heartbeatWithMessages() throws IOException {
        final int partition = 1;
        final int offsetValue = 123;
        final Offset offset = new TopicPartitionOffset("topic", partition, offsetValue);
        final ExpiringConsumer<String> consumer = mock(ExpiringConsumer.class);
        final PartitionedWriter<String> writer = new PartitionedWriter<>((ignored) -> consumer,
                mock(OffsetComputer.class), "ignored");

        writer.write(Instant.EPOCH, offset, "Message");
        writer.heartbeat(partition, offset);

        verify(consumer, never()).write(eq(null), any(Offset.class));
        verify(consumer, never()).close();
    }

    @Test(timeout = 3000)
    public void writerExpirer() throws InterruptedException {
        final PartitionedWriter<String> firstConsumer = mock(PartitionedWriter.class);
        final PartitionedWriter<String> secondConsumer = mock(PartitionedWriter.class);
        final PartitionedWriter.Expirer<String> expirer = new PartitionedWriter.Expirer<>(
                Arrays.asList(firstConsumer, secondConsumer), Duration.ofMillis(1));

        expirer.start();

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

        expirer.start();
        Thread.sleep(500);
        expirer.stop().join();
    }

    @Test(timeout = 3000)
    public void writerExpirerStopWhileWaiting() throws InterruptedException {
        final PartitionedWriter.Expirer<String> expirer = new PartitionedWriter.Expirer<>(
                Collections.singleton(mock(PartitionedWriter.class)), Duration.ofHours(42));

        expirer.start();
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
