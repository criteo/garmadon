package com.criteo.hadoop.garmadon.jvm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Used to register and collect specific statistics implemented through {@link AbstractStatistic}
 */
public abstract class StatisticCollector<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCollector.class);

    private final List<AbstractStatistic> statistics = new CopyOnWriteArrayList<>();
    private final List<Supplier<AbstractStatistic>> delayedRegisterStatistics = new CopyOnWriteArrayList<>();
    private final BiConsumer<Long, T> printer;
    private final StatisticsSink<T> sink;
    private final AtomicInteger retries = new AtomicInteger(0);

    public StatisticCollector(BiConsumer<Long, T> printer, StatisticsSink<T> sink) {
        this.printer = printer;
        this.sink = sink;
    }

    public void register(AbstractStatistic statistic) {
        statistics.add(statistic);
    }

    public void delayRegister(Supplier<AbstractStatistic> registerCallback) {
        delayedRegisterStatistics.add(registerCallback);
        retries.addAndGet(10);
    }

    public void collect() {
        processDelayedRegisterStatistics();
        for (int i = 0; i < statistics.size(); i++) { // No foreach to avoid iterator allocation (critical path)
            AbstractStatistic statistic = statistics.get(i);
            try {
                statistic.collect(sink);
            } catch (Throwable ignored) {
                // swallow any issue during stats collection
            }
        }
        if (printer != null) printer.accept(System.currentTimeMillis(), sink.flush());
    }

    public void unregister() {
        for (AbstractStatistic statistic : statistics) {
            try {
                statistic.close();
            } catch (Throwable ignored) {
            }
        }
        statistics.clear();
    }

    private void processDelayedRegisterStatistics() {
        if (delayedRegisterStatistics.isEmpty()) return;
        List<Supplier<AbstractStatistic>> list = new ArrayList<>(delayedRegisterStatistics);
        delayedRegisterStatistics.clear();
        for (Supplier<AbstractStatistic> callback : list) {
            try {
                AbstractStatistic statistic = callback.get();
                if (statistic != null) statistics.add(statistic);
            } catch (Throwable ex) {
                if (retries.decrementAndGet() > 0) {
                    LOGGER.debug("Cannot register statistic, retrying: " + ex.toString());
                    delayedRegisterStatistics.add(callback);
                } else LOGGER.debug("Cannot register statistic: ", ex);
            }
        }
    }
}
