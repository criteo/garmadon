package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.scheduler.*;
import org.apache.spark.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.util.HashMap;
import java.util.HashSet;

/**
 * There is a complexity due to the tracking of blocks per executor
 * Maybe it is not usefull as it seems to only remove object from structure
 * But spark is not clear about what block update events are fired in what condition
 * The class mimics AppStatusListener behavior, remove any code unnecessary for our purpose
 */
public class GarmadonSparkStorageStatusListener extends SparkListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonSparkStorageStatusListener.class);

    private final TriConsumer<Long, Header, Object> eventHandler;
    private final Header.SerializedHeader header;

    private final HashMap<Integer, GarmadonRDDStorageInfo> liveRDDs = new HashMap<>();
    private final HashMap<String, GarmadonExecutorStorageInfo> liveExecutors = new HashMap<>();

    //Global RDD storage
    private static class GarmadonRDDStorageInfo {
        private String name;

        private long offHeapMemoryUsed = 0;
        private long memoryUsed = 0;
        private long diskUsed = 0;

        private HashMap<String, GarmadonRDDStorageDistribution> distributions = new HashMap<>();
        private HashMap<String, RDDPartition> partitions = new HashMap<>();

        GarmadonRDDStorageInfo(String name) {
            this.name = name;
        }
    }

    //Per executor RDD storage
    private static class GarmadonRDDStorageDistribution {
        private String executorId;

        private long offHeapMemoryUsed = 0;
        private long memoryUsed = 0;
        private long diskUsed = 0;

        GarmadonRDDStorageDistribution(String executorId) {
            this.executorId = executorId;
        }
    }

    private static class RDDPartition {
        private HashSet<String> executors = new HashSet<>();
    }

    //Global executor storage
    private static class GarmadonExecutorStorageInfo {
        private String host;

        private long rddOffHeapMemoryUsed = 0;
        private long rddMemoryUsed = 0;
        private long rddDiskUsed = 0;

        private long streamOffHeapMemoryUsed = 0;
        private long streamMemoryUsed = 0;
        private long streamDiskUsed = 0;

        private long broadcastOffHeapMemoryUsed = 0;
        private long broadcastMemoryUsed = 0;
        private long broadcastDiskUsed = 0;

        private int rddBlocks = 0;

        GarmadonExecutorStorageInfo(String host) {
            this.host = host;
        }
    }

    public GarmadonSparkStorageStatusListener() {
        this(SparkListernerConf.getInstance().getEventHandler(), SparkListernerConf.getInstance().getHeader());
    }

    public GarmadonSparkStorageStatusListener(TriConsumer<Long, Header, Object> eventHandler, Header.SerializedHeader header) {
        this.eventHandler = eventHandler;
        this.header = header;

        //executor driver exists but cannot be caught by an executor added event
        liveExecutors.put("driver", new GarmadonExecutorStorageInfo(header.getHostname()));
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted event) {
        Iterator<RDDInfo> it = event.stageInfo().rddInfos().iterator();
        while (it.hasNext()) {
            RDDInfo info = it.next();
            if (info.storageLevel().isValid()) {
                liveRDDs.computeIfAbsent(info.id(), key -> new GarmadonRDDStorageInfo(info.name()));
            }
        }
    }

    /**
     * Unpersist can happen outside of a RDD computation, thus we fire events also at this moment
     */
    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD event) {
        GarmadonRDDStorageInfo info = liveRDDs.remove(event.rddId());
        if (info != null) {
            //remove this rdd from the storage accounted by executor
            info.distributions.values().forEach(distribution -> {
                updateExecutorRDDMemoryStatus(distribution.executorId, -info.diskUsed, -info.memoryUsed, -info.offHeapMemoryUsed);
            });
            //remove blocks from the executors, one block per partition
            info.partitions.values().forEach(partition -> {
                partition.executors.forEach(executorId -> {
                    GarmadonExecutorStorageInfo exec = liveExecutors.get(executorId);
                    if (exec != null) {
                        exec.rddBlocks -= 1;
                    }
                });
            });
        }

        fireEvents(System.currentTimeMillis());
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded event) {
        liveExecutors.computeIfAbsent(event.executorId(), key -> new GarmadonExecutorStorageInfo(event.executorInfo().executorHost()));
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved event) {
        liveExecutors.remove(event.executorId());
        liveRDDs.values().forEach(info -> info.distributions.remove(event.executorId()));
    }

    /**
     * onBlockUpdated would fire way too many events while giving a precision not needed so far
     * onTaskEnd is used as the trigger to send events (one per rdd and one per executor)
     * Doing so, we avoid a mechanism based on another thread firing events at a regular rate
     * thus we avoid synchronization
     */
    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        fireEvents(taskEnd.taskInfo().finishTime());
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated event) {
        StorageLevel storageLevel = event.blockUpdatedInfo().storageLevel();
        long diskDelta = event.blockUpdatedInfo().diskSize() * (storageLevel.useDisk() ? 1 : -1);
        long memoryDelta = storageLevel.useOffHeap() ? 0 : event.blockUpdatedInfo().memSize() * (storageLevel.useMemory() ? 1 : -1);
        long offHeapMemoryDelta = storageLevel.useOffHeap() ? event.blockUpdatedInfo().memSize() * (storageLevel.useMemory() ? 1 : -1) : 0;

        BlockId blockId = event.blockUpdatedInfo().blockId();
        String executorId = event.blockUpdatedInfo().blockManagerId().executorId();
        if (blockId instanceof RDDBlockId) {
            updateRDDMemoryStatus((RDDBlockId) blockId, storageLevel, executorId, diskDelta, memoryDelta, offHeapMemoryDelta);
            updateExecutorRDDMemoryStatus(executorId, diskDelta, memoryDelta, offHeapMemoryDelta);
        }
        if (blockId instanceof StreamBlockId) {
            updateExecutorStreamMemoryStatus(executorId, diskDelta, memoryDelta, offHeapMemoryDelta);
        }
        if (blockId instanceof BroadcastBlockId) {
            updateExecutorBroadcastMemoryStatus(executorId, diskDelta, memoryDelta, offHeapMemoryDelta);
        }
    }

    private void updateRDDMemoryStatus(RDDBlockId blockId, StorageLevel storageLevel, String executorId,
                                       long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonRDDStorageInfo rddInfo = liveRDDs.get(blockId.rddId());
        if (rddInfo != null) {
            //Global info
            rddInfo.offHeapMemoryUsed = addDelta(rddInfo.offHeapMemoryUsed, offHeapMemoryDelta);
            rddInfo.memoryUsed = addDelta(rddInfo.memoryUsed, memoryDelta);
            rddInfo.diskUsed = addDelta(rddInfo.diskUsed, diskDelta);

            //partition update, one partition == one block
            RDDPartition partition = rddInfo.partitions.computeIfAbsent(blockId.name(), key -> new RDDPartition());

            //compute block delta as number of block is used to remove or not a distribution from an RDD
            int blockDelta = 0;
            if (storageLevel.isValid()) {
                if (!partition.executors.contains(executorId)) {
                    blockDelta += 1;
                    partition.executors.add(executorId);
                }
            } else {
                blockDelta -= 1;
                partition.executors.remove(executorId);
            }

            //remove a partition that leaves on no executor
            if (partition.executors.isEmpty()) {
                rddInfo.partitions.remove(blockId.name());
            }

            //update distribution over the executorId if relevant
            GarmadonExecutorStorageInfo exec = liveExecutors.get(executorId);
            if (exec != null) {
                if (exec.rddBlocks + blockDelta > 0) {
                    GarmadonRDDStorageDistribution distribution = rddInfo.distributions.computeIfAbsent(executorId, GarmadonRDDStorageDistribution::new);
                    distribution.diskUsed = addDelta(distribution.diskUsed, diskDelta);
                    distribution.memoryUsed = addDelta(distribution.memoryUsed, memoryDelta);
                    distribution.offHeapMemoryUsed = addDelta(distribution.offHeapMemoryUsed, offHeapMemoryDelta);
                } else {
                    //no block on the executor, we can remove the distribution
                    rddInfo.distributions.remove(executorId);
                }

                //update executor number of blocks
                exec.rddBlocks += blockDelta;
            }
        }
    }

    private void updateExecutorRDDMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.rddOffHeapMemoryUsed = addDelta(executorInfo.rddOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.rddMemoryUsed = addDelta(executorInfo.rddMemoryUsed, memoryDelta);
            executorInfo.rddDiskUsed = addDelta(executorInfo.rddDiskUsed, diskDelta);
        }
    }

    private void updateExecutorStreamMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.streamOffHeapMemoryUsed = addDelta(executorInfo.streamOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.streamMemoryUsed = addDelta(executorInfo.streamMemoryUsed, memoryDelta);
            executorInfo.streamDiskUsed = addDelta(executorInfo.streamDiskUsed, diskDelta);
        }
    }

    private void updateExecutorBroadcastMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.broadcastOffHeapMemoryUsed = addDelta(executorInfo.broadcastOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.broadcastMemoryUsed = addDelta(executorInfo.broadcastMemoryUsed, memoryDelta);
            executorInfo.broadcastDiskUsed = addDelta(executorInfo.broadcastDiskUsed, diskDelta);
        }
    }

    //Uses same mechanics as spark's AppStatusListener
    private long addDelta(long value, long delta) {
        return Math.max(0, value + delta);
    }

    private void fireEvents(Long timestamp) {
        liveRDDs.forEach((id, rddInfo) -> sendRDDStorageStatusEvent(timestamp, rddInfo));
        liveExecutors.forEach((id, executorInfo) -> sendExecutorStorageStatusEvent(timestamp, id, executorInfo));
    }

    private void sendRDDStorageStatusEvent(Long timestamp, GarmadonRDDStorageInfo rddInfo) {
        SparkEventProtos.RDDStorageStatus event = SparkEventProtos.RDDStorageStatus.newBuilder()
            .setRddName(rddInfo.name)
            .setDiskUsed(rddInfo.diskUsed)
            .setMemoryUsed(rddInfo.memoryUsed)
            .setOffHeapMemoryUsed(rddInfo.offHeapMemoryUsed)
            .build();

        this.eventHandler.accept(timestamp, this.header, event);
    }

    private void sendExecutorStorageStatusEvent(Long timestamp, String executorId, GarmadonExecutorStorageInfo executorInfo) {
        SparkEventProtos.ExecutorStorageStatus event = SparkEventProtos.ExecutorStorageStatus.newBuilder()
            .setExecutorHostname(executorInfo.host)
            .setRddDiskUsed(executorInfo.rddDiskUsed)
            .setRddMemoryUsed(executorInfo.rddMemoryUsed)
            .setRddOffHeapMemoryUsed(executorInfo.rddOffHeapMemoryUsed)
            .setStreamDiskUsed(executorInfo.streamDiskUsed)
            .setStreamMemoryUsed(executorInfo.streamMemoryUsed)
            .setStreamOffHeapMemoryUsed(executorInfo.streamOffHeapMemoryUsed)
            .setBroadcastDiskUsed(executorInfo.broadcastDiskUsed)
            .setBroadcastMemoryUsed(executorInfo.broadcastMemoryUsed)
            .setBroadcastOffHeapMemoryUsed(executorInfo.broadcastOffHeapMemoryUsed)
            .build();

        Header headerWithExecutorId = header.cloneAndOverride(Header.newBuilder().withExecutorId(executorId).build());

        this.eventHandler.accept(timestamp, headerWithExecutorId, event);
    }

}
