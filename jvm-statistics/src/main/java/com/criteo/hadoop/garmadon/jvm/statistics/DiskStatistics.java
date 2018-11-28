package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;

import java.util.ArrayList;
import java.util.List;

class DiskStatistics extends AbstractStatistic {
    private static final String DISK_HEADER = "disk";
    private static final String DISK_READS_SUFFIX = "_reads";
    private static final String DISK_READBYTES_SUFFIX = "_readbytes";
    private static final String DISK_WRITES_SUFFIX = "_writes";
    private static final String DISK_WRITEBYTES_SUFFIX = "_writebytes";

    private final List<HWDiskStore> disks = new ArrayList<>();
    private long[] previousReads;
    private long[] previousReadBytes;
    private long[] previousWrites;
    private long[] previousWriteBytes;

    DiskStatistics() {
        super(DISK_HEADER);
        for (HWDiskStore disk : new SystemInfo().getHardware().getDiskStores()) {
            if (disk.getReads() == 0 && disk.getWrites() == 0) continue;  // nothing happens on this disk since boot
            disks.add(disk);
        }
        previousReads = new long[disks.size()];
        previousReadBytes = new long[disks.size()];
        previousWrites = new long[disks.size()];
        previousWriteBytes = new long[disks.size()];

        int i = 0;
        for (HWDiskStore disk : disks) {
            previousReads[i] = disk.getReads();
            previousReadBytes[i] = disk.getReadBytes();
            previousWrites[i] = disk.getWrites();
            previousWriteBytes[i] = disk.getWriteBytes();
            i++;
        }

    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        int i = 0;
        for (HWDiskStore disk : disks) {
            String name = disk.getName().replace("/dev/", "");
            disk.updateDiskStats();
            sink.add(name + DISK_READS_SUFFIX, disk.getReads() - previousReads[i]);
            previousReads[i] = disk.getReads();
            sink.add(name + DISK_READBYTES_SUFFIX, disk.getReadBytes() - previousReadBytes[i]);
            previousReadBytes[i] = disk.getReadBytes();
            sink.add(name + DISK_WRITES_SUFFIX, disk.getWrites() - previousWrites[i]);
            previousWrites[i] = disk.getWrites();
            sink.add(name + DISK_WRITEBYTES_SUFFIX, disk.getWriteBytes() - previousWriteBytes[i]);
            previousWriteBytes[i] = disk.getWriteBytes();
            i++;
        }
    }
}