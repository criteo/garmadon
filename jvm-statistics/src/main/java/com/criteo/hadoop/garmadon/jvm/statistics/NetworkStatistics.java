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
    private static final String NETWORK_PKT_RECV_SUFFIX = "_pktrx";
    private static final String NETWORK_PKT_SENT_SUFFIX = "_pkttx";
    private static final String NETWORK_ERRORS_IN_SUFFIX = "_errin";
    private static final String NETWORK_ERRORS_OUT_SUFFIX = "_errout";

    private final List<NicInfo> nics = new ArrayList<>();

    NetworkStatistics() {
        super(NETWORK_HEADER);
        for (NetworkIF nic : new SystemInfo().getHardware().getNetworkIFs()) {
            if (nic.getBytesSent() == 0 && nic.getBytesRecv() == 0) {
                // nothing happens on this nic since boot
                continue;
            }
            nics.add(new NicInfo(nic));
        }
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        for (NicInfo nic : nics) {
            nic.dumpStats(sink);
        }
    }

    private static class NicInfo {
        private final NetworkIF nic;
        private final String recvProp;
        private final String sentProp;
        private final String pktRecvProp;
        private final String pktSentProp;
        private final String errInProp;
        private final String errOutProp;
        private long previousRx;
        private long previousTx;
        private long previousPktRx;
        private long previousPktTx;
        private long previousErrIn;
        private long previousErrOut;

        NicInfo(NetworkIF nic) {
            this.nic = nic;
            this.recvProp = nic.getName() + NETWORK_RECV_SUFFIX;
            this.sentProp = nic.getName() + NETWORK_SENT_SUFFIX;
            this.pktRecvProp = nic.getName() + NETWORK_PKT_RECV_SUFFIX;
            this.pktSentProp = nic.getName() + NETWORK_PKT_SENT_SUFFIX;
            this.errInProp = nic.getName() + NETWORK_ERRORS_IN_SUFFIX;
            this.errOutProp = nic.getName() + NETWORK_ERRORS_OUT_SUFFIX;
            this.previousRx = nic.getBytesRecv();
            this.previousTx = nic.getBytesSent();
            this.previousPktRx = nic.getPacketsRecv();
            this.previousPktTx = nic.getPacketsSent();
            this.previousErrIn = nic.getInErrors();
            this.previousErrOut = nic.getOutErrors();
        }

        void dumpStats(StatisticsSink sink) {
            nic.updateNetworkStats();
            sink.add(recvProp, nic.getBytesRecv() - previousRx);
            previousRx = nic.getBytesRecv();
            sink.add(sentProp, nic.getBytesSent() - previousTx);
            previousTx = nic.getBytesSent();
            sink.add(pktRecvProp, nic.getPacketsRecv() - previousPktRx);
            previousPktRx = nic.getPacketsRecv();
            sink.add(pktSentProp, nic.getPacketsSent() - previousPktTx);
            previousPktTx = nic.getPacketsSent();
            sink.add(errInProp, nic.getInErrors() - previousErrIn);
            previousErrIn = nic.getInErrors();
            sink.add(errOutProp, nic.getOutErrors() - previousErrOut);
            previousErrOut = nic.getOutErrors();
        }
    }
}