package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class ExpiringConsumerTest {
    private static final Offset DUMMY_OFFSET = buildOffset(12);

    @Test
    public void expiredAfterTooManyMessages() throws IOException, SQLException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ofMinutes(10), 3);

        for (int i = 0; i < 3; i++) {
            Assert.assertFalse(consumer.isExpired());
            consumer.write(1234567890L, 1, DUMMY_OFFSET);
        }
        Assert.assertTrue(consumer.isExpired());

        // Expiring doesn't close
        verify(innerMock, times(0)).close();
    }

    @Test
    public void expiredAfterTooMuchIdleTime() throws InterruptedException, IOException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ofSeconds(1), 10);

        consumer.write(1234567890L, 12, DUMMY_OFFSET);
        Assert.assertFalse(consumer.isExpired());
        Thread.sleep(1001);
        Assert.assertTrue(consumer.isExpired());
    }

    @Test
    public void sendAnotherMessageAfterTimeExpiration() throws InterruptedException, IOException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ofSeconds(1), 10);

        Thread.sleep(1001);
        Assert.assertTrue(consumer.isExpired());

        // Sending another message doesn't renew the expiration delay
        consumer.write(1234567890L, 12, DUMMY_OFFSET);
        Assert.assertTrue(consumer.isExpired());
    }

    @Test
    public void expiredFromTheStart() throws InterruptedException {
        final ExpiringConsumer<Integer> noTimeConsumer = new ExpiringConsumer<>(null, Duration.ZERO, 10);
        Thread.sleep(1);
        Assert.assertTrue(noTimeConsumer.isExpired());

        final ExpiringConsumer<Integer> noMessageConsumer = new ExpiringConsumer<>(null, Duration.ofMinutes(10), 0);
        Assert.assertTrue(noMessageConsumer.isExpired());

        final ExpiringConsumer<Integer> allZerosConsumer = new ExpiringConsumer<>(null, Duration.ZERO, 0);
        Thread.sleep(1);
        Assert.assertTrue(allZerosConsumer.isExpired());
    }

    @Test
    public void messagesArePassedToInnerWriter() throws IOException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ZERO, 5);
        final List<Offset> offsets = Arrays.asList(buildOffset(11), buildOffset(12), buildOffset(13));

        for (int i = 0; i < 3; i++) {
            consumer.write(1234567890L, i, offsets.get(i));
        }

        final InOrder inOrder = inOrder(innerMock);
        for (int i = 0; i < 3; i++) {
            inOrder.verify(innerMock).write(1234567890L, i, offsets.get(i));
        }
        verifyNoMoreInteractions(innerMock);
    }

    @Test
    public void closeWithNoMessage() throws IOException, SQLException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ZERO, 10);

        consumer.close();

        verify(innerMock, times(1)).close();
    }

    @Test
    public void closeWithMessages() throws IOException, SQLException {
        final CloseableWriter<Integer> innerMock = mock(CloseableWriter.class);
        final ExpiringConsumer<Integer> consumer = new ExpiringConsumer<>(innerMock, Duration.ZERO, 10);

        consumer.write(1234567890L, 1, DUMMY_OFFSET);
        consumer.write(1234567890L, 2, DUMMY_OFFSET);
        consumer.close();
    }

    private static Offset buildOffset(long offset) {
        final Offset offsetMock = mock(Offset.class);

        doReturn(offset).when(offsetMock).getOffset();

        return offsetMock;
    }
}
