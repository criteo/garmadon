package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import oshi.SystemInfo;
import oshi.hardware.NetworkIF;

import java.util.ArrayList;
import java.util.List;

class NetworkStatistics extends AbstractStatistic {
    private static final String NETWORK_HEADER = "network";
    private static final String NETWORK_RECV_SUFFIX = "_rx";
    private static final String NETWORK_SENT_SUFFIX = "_tx";

    private final List<NetworkIF> nics = new ArrayList<>();
    private long[] previous_rx;
    private long[] previous_tx;

    NetworkStatistics() {
        super(NETWORK_HEADER);
        for (NetworkIF nic : new SystemInfo().getHardware().getNetworkIFs()) {
            if (nic.getBytesSent() == 0 && nic.getBytesRecv() == 0) // nothing happens on this nic since boot
                continue;
            nics.add(nic);
        }
        previous_rx = new long[nics.size()];
        previous_tx = new long[nics.size()];

        int i = 0;
        for (NetworkIF nic : nics) {
            previous_rx[i] = nic.getBytesRecv();
            previous_tx[i] = nic.getBytesSent();
            i++;
        }
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        int i = 0;
        for (NetworkIF nic : nics) {
            nic.updateNetworkStats();
            sink.add(nic.getName() + NETWORK_RECV_SUFFIX, nic.getBytesRecv() - previous_rx[i]);
            previous_rx[i] = nic.getBytesRecv();
            sink.add(nic.getName() + NETWORK_SENT_SUFFIX, nic.getBytesSent() - previous_tx[i]);
            previous_tx[i] = nic.getBytesSent();
            i++;
        }
    }
}