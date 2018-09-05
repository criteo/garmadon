package com.criteo.jvm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Used to register and collect specific statistics implemented through {@link AbstractStatistic}
 */
public abstract class StatisticCollector<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCollector.class);

    protected final List<AbstractStatistic> statistics = new CopyOnWriteArrayList<>();
    protected final List<Supplier<AbstractStatistic>> delayedRegisterStatistics = new CopyOnWriteArrayList<>();
    protected final Consumer<T> printer;
    protected final StatisticsSink<T> sink;
    private final AtomicInteger retries = new AtomicInteger(0);

    public StatisticCollector(Consumer<T> printer, StatisticsSink<T> sink) {
        this.printer = printer;
        this.sink = sink;
    }

    public void register(AbstractStatistic statistic) {
        statistics.add(statistic);
    }

    public void delayRegister(Supplier<AbstractStatistic> registerCallback) {
        delayedRegisterStatistics.add(registerCallback);
        retries.addAndGet(3); // this is an approximation of 3 retries per statistic to avoid using a more complex structure
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
        if (printer != null)
            printer.accept(sink.flush());
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
        if (delayedRegisterStatistics.isEmpty())
            return;
        List<Supplier<AbstractStatistic>> list = new ArrayList<>(delayedRegisterStatistics);
        delayedRegisterStatistics.clear();
        for (Supplier<AbstractStatistic> callback : list) {
            try {
                AbstractStatistic statistic = callback.get();
                if (statistic != null)
                    statistics.add(statistic);
            } catch (Throwable ex) {
                if (retries.decrementAndGet() > 0) {
                    delayedRegisterStatistics.add(callback);
                    LOGGER.warn("Cannot register statistic: " + ex.toString());
                }
                else
                    LOGGER.error("Cannot register statistic: ", ex);
            }
        }
    }
}
