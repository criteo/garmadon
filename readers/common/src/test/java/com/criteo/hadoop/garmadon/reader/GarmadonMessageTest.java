package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GarmadonMessageTest {

    @Test
    public void testToMapNormalized() {
        long timestampMillis = System.currentTimeMillis();
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder().build();
        DataAccessEventProtos.FsEvent.Builder fsEvent = DataAccessEventProtos.FsEvent.newBuilder()
                .setSrcPath("hdfs://root/src_file")
                .setDstPath("hdfs://root/dst_file")
                .setAction("FS_RENAME")
                .setHdfsUser("mathieu")
                .setUri("hdfs://root");

        GarmadonMessage message = new GarmadonMessage(GarmadonSerialization.TypeMarker.FS_EVENT, timestampMillis, header, fsEvent.build(), null);

        Map<String, Object> map = message.toMap(false, false);

        HashMap<Object, Object> expected = new HashMap<>();

        // scheme and host removed
        expected.put("src_path", "/src_file");
        expected.put("dst_path", "/dst_file");
        // normalized uri
        expected.put("uri", "hdfs://preprod-pa4");

        expected.put("hdfs_user", "mathieu");
        expected.put("action", "FS_RENAME");
        expected.put("timestamp", timestampMillis);

        TestCase.assertEquals(expected, map);
    }

    @Test
    public void testToMapNormalizedWithPortInfo() {
        long timestampMillis = System.currentTimeMillis();
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder().build();
        DataAccessEventProtos.FsEvent.Builder fsEvent = DataAccessEventProtos.FsEvent.newBuilder()
                .setSrcPath("hdfs://root:8020/src_file")
                .setDstPath("hdfs://root:8020/dst_file")
                .setAction("FS_RENAME")
                .setHdfsUser("mathieu")
                .setUri("hdfs://root:8020");

        GarmadonMessage message = new GarmadonMessage(GarmadonSerialization.TypeMarker.FS_EVENT, timestampMillis, header, fsEvent.build(), null);

        Map<String, Object> map = message.toMap(false, false);

        HashMap<Object, Object> expected = new HashMap<>();

        // scheme and host removed
        expected.put("src_path", "/src_file");
        expected.put("dst_path", "/dst_file");
        // normalized uri
        expected.put("uri", "hdfs://preprod-pa4");

        expected.put("hdfs_user", "mathieu");
        expected.put("action", "FS_RENAME");
        expected.put("timestamp", timestampMillis);

        TestCase.assertEquals(expected, map);
    }

    @Test
    public void testToMapOnInfinityDoubleReturnMaxNegativeValue() {
        long timestampMillis = System.currentTimeMillis();
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder().build();
        FlinkEventProtos.OperatorEvent.Builder operatorEvent = FlinkEventProtos.OperatorEvent.newBuilder()
            .setRecordsLagMax(Double.NEGATIVE_INFINITY);

        GarmadonMessage message = new GarmadonMessage(GarmadonSerialization.TypeMarker.FLINK_OPERATOR_EVENT, timestampMillis, header, operatorEvent.build(), null);

        Map<String, Object> map = message.toMap(true, true);

        TestCase.assertEquals(-Double.MAX_VALUE, map.get("records_lag_max"));
    }
}
