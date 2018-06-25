package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import com.criteo.hadoop.garmadon.schema.events.StateEvent;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileHeuristicTest {

    private FileHeuristic heuristic;
    private HeuristicsResultDB db;

    @Before
    public void setUp() {
        this.db = mock(HeuristicsResultDB.class);
        this.heuristic = new FileHeuristic(db);
    }

    @Test
    public void FileHeuristic_should_update_delete_counter_for_every_FileEvent_delete_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsEvent.Action.DELETE);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.deleted.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.deleted.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_read_counter_for_every_FileEvent_read_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsEvent.Action.READ);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.read.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.read.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_write_counter_for_every_FileEvent_write_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsEvent.Action.WRITE);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.written.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.written.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_rename_counter_for_every_FileEvent_rename_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsEvent.Action.RENAME);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.renamed.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.renamed.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_ignore_unknown_actions() {
        DataAccessEventProtos.FsEvent unknowActionEvent = newFsEvent("whatever");

        heuristic.compute("app_1", "att_1", "cid_1", unknowActionEvent);
    }

    @Test
    public void FileHeuristic_should_write_heuristic_results_when_the_last_container_ends() {
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsEvent.Action.DELETE));
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsEvent.Action.WRITE));
        heuristic.compute("app_1", "att_1", "cid_2", newFsEvent(FsEvent.Action.RENAME));
        heuristic.compute("app_1", "att_1", "cid_3", newFsEvent(FsEvent.Action.READ));

        heuristic.compute("app_2", "att_1", "cid_1", newFsEvent(FsEvent.Action.READ));
        heuristic.compute("app_2", "att_1", "cid_2", newFsEvent(FsEvent.Action.WRITE));

        //end containers
        heuristic.compute("app_1", "att_1", "cid_1", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        heuristic.compute("app_1", "att_1", "cid_2", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        heuristic.compute("app_2", "att_1", "cid_1", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        HeuristicResult expectedResults = new HeuristicResult("app_1", "att_1", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "1");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "1");

        heuristic.compute("app_1", "att_1", "cid_3", newEndEvent());
        verify(db).createHeuristicResult(expectedResults);
        reset(db);

        heuristic.compute("app_1", "att_1", "cid_4", newEndEvent());
        verify(db, never()).createHeuristicResult(any());
    }

    @Test
    public void FileHeuristic_should_write_2_heuristic_on_different_attempt() {
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsEvent.Action.DELETE));
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsEvent.Action.WRITE));
        heuristic.compute("app_1", "att_1", "cid_2", newFsEvent(FsEvent.Action.RENAME));
        heuristic.compute("app_1", "att_1", "cid_3", newFsEvent(FsEvent.Action.READ));

        heuristic.compute("app_1", "att_2", "cid_1", newFsEvent(FsEvent.Action.READ));
        heuristic.compute("app_1", "att_2", "cid_2", newFsEvent(FsEvent.Action.WRITE));

        //end containers
        heuristic.compute("app_1", "att_1", "cid_1", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        heuristic.compute("app_1", "att_1", "cid_2", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        heuristic.compute("app_1", "att_2", "cid_1", newEndEvent());
        verify(db, never()).createHeuristicResult(any());

        HeuristicResult expectedResults = new HeuristicResult("app_1", "att_1", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "1");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "1");

        heuristic.compute("app_1", "att_1", "cid_3", newEndEvent());
        verify(db).createHeuristicResult(expectedResults);
        reset(db);

        expectedResults = new HeuristicResult("app_1", "att_2", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "0");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "0");

        heuristic.compute("app_1", "att_2", "cid_2", newEndEvent());
        verify(db).createHeuristicResult(expectedResults);
        reset(db);
    }

    private DataAccessEventProtos.StateEvent newEndEvent() {
        return DataAccessEventProtos.StateEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setState(StateEvent.State.END.toString())
                .build();
    }

    private DataAccessEventProtos.FsEvent newFsEvent(FsEvent.Action action) {
        return newFsEvent(action.toString());
    }

    private DataAccessEventProtos.FsEvent newFsEvent(String action) {
        return DataAccessEventProtos.FsEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(action)
                .build();
    }


}
