package com.criteo.jvm.statistics;

import com.criteo.jvm.AbstractStatistic;
import com.criteo.jvm.StatisticsSink;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class DiskStatistics extends AbstractStatistic {
    private static final String DISK_HEADER = "disk";
    private static final String DISK_READS_SUFFIX = "_reads";
    private static final String DISK_READBYTES_SUFFIX = "_readbytes";
    private static final String DISK_WRITES_SUFFIX = "_writes";
    private static final String DISK_WRITEBYTES_SUFFIX = "_writebytes";

    private final List<HWDiskStore> disks = new ArrayList<>();
    private long[] previous_reads;
    private long[] previous_readbytes;
    private long[] previous_writes;
    private long[] previous_writebytes;

    DiskStatistics() {
        super(DISK_HEADER);
        for (HWDiskStore disk : new SystemInfo().getHardware().getDiskStores()) {
            if (disk.getReads() == 0 && disk.getWrites() == 0)  // nothing happens on this disk since boot
                continue;
            disks.add(disk);
        }
        previous_reads = new long[disks.size()];
        previous_readbytes = new long[disks.size()];
        previous_writes = new long[disks.size()];
        previous_writebytes = new long[disks.size()];

        int i = 0;
        for (HWDiskStore disk : disks) {
            previous_reads[i] = disk.getReads();
            previous_readbytes[i] = disk.getReadBytes();
            previous_writes[i] = disk.getWrites();
            previous_writebytes[i] = disk.getWriteBytes();
            i++;
        }

    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        int i = 0;
        for (HWDiskStore disk : disks) {
            String name = disk.getName().replace("/dev/", "");
            disk.updateDiskStats();
            sink.add(name + DISK_READS_SUFFIX, disk.getReads() - previous_reads[i]);
            previous_reads[i] = disk.getReads();
            sink.add(name + DISK_READBYTES_SUFFIX, disk.getReadBytes() - previous_readbytes[i]);
            previous_readbytes[i] = disk.getReadBytes();
            sink.add(name + DISK_WRITES_SUFFIX, disk.getWrites() - previous_writes[i]);
            previous_writes[i] = disk.getWrites();
            sink.add(name + DISK_WRITEBYTES_SUFFIX, disk.getWriteBytes() - previous_writebytes[i]);
            previous_writebytes[i] = disk.getWriteBytes();
            i++;
        }
    }
}