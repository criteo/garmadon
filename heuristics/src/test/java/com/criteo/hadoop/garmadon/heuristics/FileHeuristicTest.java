package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FileHeuristicTest {

    private FileHeuristic heuristic;

    @Before
    public void setUp(){
        this.heuristic = new FileHeuristic();
    }

    @Test
    public void FileHeuristic_should_update_delete_counter_for_every_FileEvent_delete_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(FsEvent.Action.DELETE.toString())
                .build();

        heuristic.compute("app_1", "cid_1", event);
        assertThat(heuristic.deleted.forApp("app_1").getCount(), is(1));

        heuristic.compute("app_1", "cid_2", event);
        assertThat(heuristic.deleted.forApp("app_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_read_counter_for_every_FileEvent_read_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(FsEvent.Action.READ.toString())
                .build();

        heuristic.compute("app_1", "cid_1", event);
        assertThat(heuristic.read.forApp("app_1").getCount(), is(1));

        heuristic.compute("app_1", "cid_2", event);
        assertThat(heuristic.read.forApp("app_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_write_counter_for_every_FileEvent_write_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(FsEvent.Action.WRITE.toString())
                .build();

        heuristic.compute("app_1", "cid_1", event);
        assertThat(heuristic.written.forApp("app_1").getCount(), is(1));

        heuristic.compute("app_1", "cid_2", event);
        assertThat(heuristic.written.forApp("app_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_rename_counter_for_every_FileEvent_rename_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(FsEvent.Action.RENAME.toString())
                .build();

        heuristic.compute("app_1", "cid_1", event);
        assertThat(heuristic.renamed.forApp("app_1").getCount(), is(1));

        heuristic.compute("app_1", "cid_2", event);
        assertThat(heuristic.renamed.forApp("app_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_ignore_unknown_actions(){
        DataAccessEventProtos.FsEvent unknowActionEvent = DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction("whatever")
                .build();

        heuristic.compute("app_1", "cid_1", unknowActionEvent);
    }


}
