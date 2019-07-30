package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.scheduler.*;
import org.apache.spark.storage.*;
import scala.collection.Iterator;

import java.util.HashMap;
import java.util.HashSet;

import static com.criteo.hadoop.garmadon.spark.listener.ScalaUtils.emptyStringSupplier;

/**
 * There is a complexity due to the tracking of blocks per executor
 * Maybe it is not usefull as it seems to only remove object from structure
 * But spark is not clear about what block update events are fired in what condition
 * The class mimics AppStatusListener behavior, remove any code unnecessary for our purpose
 */
public class GarmadonSparkStorageStatusListener extends SparkListener {

    private final TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;

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


    /**
     * capture app info for the header when the driver is not in yarn cluster
     */
    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        header = header.cloneAndOverride(Header.newBuilder()
            .withApplicationID(applicationStart.appId().getOrElse(emptyStringSupplier))
            .withAttemptID(applicationStart.appAttemptId().getOrElse(emptyStringSupplier))
            .withApplicationName(applicationStart.appName())
            .build())
            .toSerializeHeader();
    }

    /**
     * capture new rdd information
     */
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
     * capture rdd removed from persistence
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

            //there will not be any blockUpdated event, so fire an event with size set to 0
            info.diskUsed = 0;
            info.memoryUsed = 0;
            info.offHeapMemoryUsed = 0;

            sendRDDStorageStatusEvent(System.currentTimeMillis(), info);
        }
    }

    /**
     * capture new executor info
     */
    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded event) {
        liveExecutors.computeIfAbsent(event.executorId(), key -> new GarmadonExecutorStorageInfo(event.executorInfo().executorHost()));
    }

    /**
     * capture removal of executor
     */
    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved event) {
        liveExecutors.remove(event.executorId());
        liveRDDs.values().forEach(info -> info.distributions.remove(event.executorId()));
    }

    /**
     * capture persistence mutation. Different types of blocks can be updated, they either concern RDD, in which case we update both rdd and executor info,
     * or just executors (stream, broadcast, etc...)
     */
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

            sendRDDStorageStatusEvent(System.currentTimeMillis(), rddInfo);
        }
    }

    private void updateExecutorRDDMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.rddOffHeapMemoryUsed = addDelta(executorInfo.rddOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.rddMemoryUsed = addDelta(executorInfo.rddMemoryUsed, memoryDelta);
            executorInfo.rddDiskUsed = addDelta(executorInfo.rddDiskUsed, diskDelta);

            sendExecutorStorageStatusEvent(System.currentTimeMillis(), executorId, executorInfo);
        }
    }

    private void updateExecutorStreamMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.streamOffHeapMemoryUsed = addDelta(executorInfo.streamOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.streamMemoryUsed = addDelta(executorInfo.streamMemoryUsed, memoryDelta);
            executorInfo.streamDiskUsed = addDelta(executorInfo.streamDiskUsed, diskDelta);

            sendExecutorStorageStatusEvent(System.currentTimeMillis(), executorId, executorInfo);
        }
    }

    private void updateExecutorBroadcastMemoryStatus(String executorId, long diskDelta, long memoryDelta, long offHeapMemoryDelta) {
        GarmadonExecutorStorageInfo executorInfo = liveExecutors.get(executorId);
        if (executorInfo != null) {
            executorInfo.broadcastOffHeapMemoryUsed = addDelta(executorInfo.broadcastOffHeapMemoryUsed, offHeapMemoryDelta);
            executorInfo.broadcastMemoryUsed = addDelta(executorInfo.broadcastMemoryUsed, memoryDelta);
            executorInfo.broadcastDiskUsed = addDelta(executorInfo.broadcastDiskUsed, diskDelta);

            sendExecutorStorageStatusEvent(System.currentTimeMillis(), executorId, executorInfo);
        }
    }

    //Uses same mechanics as spark's AppStatusListener
    private long addDelta(long value, long delta) {
        return Math.max(0, value + delta);
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
