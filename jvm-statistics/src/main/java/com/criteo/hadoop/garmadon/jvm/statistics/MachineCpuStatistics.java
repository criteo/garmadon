package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import com.google.common.annotations.VisibleForTesting;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

import java.util.regex.Pattern;

class MachineCpuStatistics extends AbstractStatistic {
    private static final String CPU_HEADER = "machinecpu";
    private static final String CPU_VAR_USER = "user";
    private static final String CPU_VAR_NICE = "nice";
    private static final String CPU_VAR_SYS = "sys";
    private static final String CPU_VAR_IDLE = "idle";
    private static final String CPU_VAR_IOWAIT = "iowait";
    private static final String CPU_VAR_IRQ = "irq";
    private static final String CPU_VAR_SOFTIRQ = "softirq";
    private static final String CPU_VAR_INTERRUPTS = "interrupts";
    private static final String CPU_VAR_CTXTSWITCHES = "ctxtswitches";
    private static final String CPU_VAR_CORE = "core";
    private static final Pattern COUNT_PATTERN = Pattern.compile("\\s+(\\d+)");

    private CentralProcessor proc;
    private long[] prevTicks;

    MachineCpuStatistics() {
        super(CPU_HEADER);
        proc = new SystemInfo().getHardware().getProcessor();
        prevTicks = proc.getSystemCpuLoadTicks();
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        long[] ticks = proc.getSystemCpuLoadTicks();
        collectCpuTicks(ticks, prevTicks, sink);
        prevTicks = ticks;
        double[] processorLoad = proc.getProcessorCpuLoadBetweenTicks();
        for (int cpuIdx = 0; cpuIdx < processorLoad.length; cpuIdx++) {
            sink.addPercentage(CPU_VAR_CORE + String.valueOf(cpuIdx), (int) Math.round(processorLoad[cpuIdx] * 100));
        }
        sink.add(CPU_VAR_INTERRUPTS, proc.getInterrupts());
        sink.add(CPU_VAR_CTXTSWITCHES, proc.getContextSwitches());
    }

    @VisibleForTesting
    static void collectCpuTicks(long[] ticks, long[] prevTicks, StatisticsSink sink) {
        long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long sys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long totalCpu = user + nice + sys + idle + iowait + irq + softirq;
        if (totalCpu > 0) {
            sink.addPercentage(CPU_VAR_USER, roundPercentage(user, totalCpu));
            sink.addPercentage(CPU_VAR_NICE, roundPercentage(nice, totalCpu));
            sink.addPercentage(CPU_VAR_SYS, roundPercentage(sys, totalCpu));
            sink.addPercentage(CPU_VAR_IDLE, roundPercentage(idle, totalCpu));
            sink.addPercentage(CPU_VAR_IOWAIT, roundPercentage(iowait, totalCpu));
            sink.addPercentage(CPU_VAR_IRQ, roundPercentage(irq, totalCpu));
            sink.addPercentage(CPU_VAR_SOFTIRQ, roundPercentage(softirq, totalCpu));
        }
    }

    private static int roundPercentage(long part, long total) {
        double d = 100d * part / total;
        return (int) Math.round(d);
    }
}
