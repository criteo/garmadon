package com.criteo.hadoop.garmadon.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class StatisticCollectorTest {

    private static final int RETRIES_COUNT = 2;
    private AtomicInteger retries = new AtomicInteger(RETRIES_COUNT);

    @Test
    public void collect() {
        AtomicBoolean executed = new AtomicBoolean(false);

        TestStatisticCollector statisticCollector = new TestStatisticCollector((t, s) -> {
            if (retries.get() >= 0)
                Assert.assertEquals("foo[name=value]", s);
            else
                Assert.assertEquals("foo[name=value], delayed[name=value]", s);
            executed.set(true);
        });
        statisticCollector.register(new TestStatistic("foo"));
        statisticCollector.delayRegister(this::createTestStatistic);
        for (int i = 0; i < RETRIES_COUNT; i++)
            statisticCollector.collect();
        Assert.assertTrue(executed.get());
    }

    private TestStatistic createTestStatistic() {
        if (retries.get() > 0) {
            retries.decrementAndGet();
            throw new RuntimeException("error");
        }
        return new TestStatistic("delayed");
    }

    private static class TestStatisticCollector extends StatisticCollector<String> {

        public TestStatisticCollector(BiConsumer<Long, String> printer) {
            super(printer, new StatisticsLog());
        }
    }

    private static class TestStatistic extends AbstractStatistic {
        private int retries;


        public TestStatistic(String name) {
            super(name);
        }

        @Override
        protected void innerCollect(StatisticsSink sink) throws Throwable {
            sink.add("name", "value");
        }
    }
}