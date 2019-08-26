package com.criteo.hadoop.garmadon.hdfs.writer;

import org.junit.Test;

import java.time.Duration;

import static org.mockito.Mockito.*;

public class ExpirerTest {

    @Test(timeout = 3000)
    public void writerExpirer() throws InterruptedException {
        final AsyncWriter<String> writer = mock(AsyncWriter.class);
        final Expirer<String> expirer = new Expirer<>(writer, Duration.ofMillis(1));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));

        Thread.sleep(500);

        verify(writer, atLeastOnce()).expireConsumers();

        expirer.stop().join();
        verify(writer, atLeastOnce()).close();
    }

    @Test(timeout = 3000)
    public void writerExpirerWithNoWriter() throws InterruptedException {
        final AsyncWriter<String> writer = mock(AsyncWriter.class);
        final Expirer<String> expirer = new Expirer<>(writer, Duration.ofMillis(10));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(500);
        expirer.stop().join();
    }

    @Test(timeout = 3000)
    public void writerExpirerStopWhileWaiting() throws InterruptedException {
        final AsyncWriter<String> writer = mock(AsyncWriter.class);
        final Expirer<String> expirer = new Expirer<>(writer, Duration.ofHours(42));

        expirer.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(1000);
        expirer.stop().join();
    }
}
