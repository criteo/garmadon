package com.criteo.hadoop.garmadon.hdfs.writer;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;

public class DelayedDailyPathComputerTest {
    @Test
    public void sameDay() {
        DelayedDailyPathComputer computer = new DelayedDailyPathComputer(Duration.ofHours(2));

        Assert.assertEquals("1987-08-13", computer.apply(LocalDateTime.parse("1987-08-13T14:00:00")));
        Assert.assertEquals("1987-08-13", computer.apply(LocalDateTime.parse("1987-08-13T02:00:00")));
    }

    @Test
    public void dayBefore() {
        DelayedDailyPathComputer computer = new DelayedDailyPathComputer(Duration.ofHours(2));

        Assert.assertEquals("1987-08-12", computer.apply(LocalDateTime.parse("1987-08-13T00:00:00")));
        Assert.assertEquals("1987-08-12", computer.apply(LocalDateTime.parse("1987-08-13T01:59:59")));
    }
}
