package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import com.criteo.hadoop.garmadon.schema.enums.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class FileHeuristicTest {

    private FileHeuristic heuristic;
    private HeuristicsResultDB db;

    @Before
    public void setUp() {
        this.db = mock(HeuristicsResultDB.class);
        this.heuristic = new FileHeuristic(db, 1000000);
    }

    @Test
    public void FileHeuristic_should_update_delete_counter_for_every_FileEvent_delete_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsAction.DELETE);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.deleted.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.deleted.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_read_counter_for_every_FileEvent_read_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsAction.READ);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.read.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.read.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_write_counter_for_every_FileEvent_write_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsAction.WRITE);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.written.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.written.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_update_rename_counter_for_every_FileEvent_rename_whatever_the_container() {

        DataAccessEventProtos.FsEvent event = newFsEvent(FsAction.RENAME);

        heuristic.compute("app_1", "att_1", "cid_1", event);
        assertThat(heuristic.renamed.forApp("app_1", "att_1").getCount(), is(1));

        heuristic.compute("app_1", "att_1", "cid_2", event);
        assertThat(heuristic.renamed.forApp("app_1", "att_1").getCount(), is(2));
    }

    @Test
    public void FileHeuristic_should_ignore_unknown_actions() {
        DataAccessEventProtos.FsEvent unknowActionEvent = newFsEvent("whatever");

        heuristic.compute("app_1", "att_1", "cid_1", unknowActionEvent);

        assertThat(heuristic.append.forApp("app_1", "att_1").getCount(), is(0));
        assertThat(heuristic.deleted.forApp("app_1", "att_1").getCount(), is(0));
        assertThat(heuristic.read.forApp("app_1", "att_1").getCount(), is(0));
        assertThat(heuristic.renamed.forApp("app_1", "att_1").getCount(), is(0));
        assertThat(heuristic.written.forApp("app_1", "att_1").getCount(), is(0));
    }

    @Test
    public void FileHeuristic_should_write_heuristic_results_when_the_last_container_ends() {
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsAction.DELETE));
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsAction.WRITE));
        heuristic.compute("app_1", "att_1", "cid_2", newFsEvent(FsAction.RENAME));
        heuristic.compute("app_1", "att_1", "cid_3", newFsEvent(FsAction.READ));
        heuristic.compute("app_1", "att_1", "cid_4", newFsEvent(FsAction.APPEND));

        heuristic.compute("app_2", "att_1", "cid_1", newFsEvent(FsAction.READ));
        heuristic.compute("app_2", "att_1", "cid_2", newFsEvent(FsAction.WRITE));

        HeuristicResult expectedResults = new HeuristicResult("app_1", "att_1", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "1");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "1");
        expectedResults.addDetail("Files appended", "1");
        expectedResults.addDetail("List status performed", "0");
        expectedResults.addDetail("Blocks added", "0");

        heuristic.onAppCompleted("app_1", "att_1");
        verify(db).createHeuristicResult(expectedResults);
    }

    @Test
    public void FileHeuristic_should_write_2_heuristic_on_different_attempt() {
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsAction.DELETE));
        heuristic.compute("app_1", "att_1", "cid_1", newFsEvent(FsAction.WRITE));
        heuristic.compute("app_1", "att_1", "cid_2", newFsEvent(FsAction.RENAME));
        heuristic.compute("app_1", "att_1", "cid_3", newFsEvent(FsAction.READ));
        heuristic.compute("app_1", "att_1", "cid_4", newFsEvent(FsAction.APPEND));

        heuristic.compute("app_1", "att_2", "cid_1", newFsEvent(FsAction.READ));
        heuristic.compute("app_1", "att_2", "cid_2", newFsEvent(FsAction.WRITE));

        HeuristicResult expectedResults = new HeuristicResult("app_1", "att_1", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "1");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "1");
        expectedResults.addDetail("Files appended", "1");
        expectedResults.addDetail("List status performed", "0");
        expectedResults.addDetail("Blocks added", "0");

        heuristic.onAppCompleted("app_1", "att_1");
        verify(db).createHeuristicResult(expectedResults);

        expectedResults = new HeuristicResult("app_1", "att_2", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        expectedResults.addDetail("Files deleted", "0");
        expectedResults.addDetail("Files read", "1");
        expectedResults.addDetail("Files written", "1");
        expectedResults.addDetail("Files renamed", "0");
        expectedResults.addDetail("Files appended", "0");
        expectedResults.addDetail("List status performed", "0");
        expectedResults.addDetail("Blocks added", "0");

        heuristic.onAppCompleted("app_1", "att_2");
        verify(db).createHeuristicResult(expectedResults);
    }

    @Test
    public void FileHeuristic_should_have_severity() {
        testFileHeuresticSeverity("app_1", HeuristicsResultDB.Severity.NONE, 100, 100);
        testFileHeuresticSeverity("app_2", HeuristicsResultDB.Severity.SEVERE, 1200000, 0);
        testFileHeuresticSeverity("app_3", HeuristicsResultDB.Severity.SEVERE, 1100000, 950000);
        testFileHeuresticSeverity("app_4", HeuristicsResultDB.Severity.MODERATE, 600000, 100);
        testFileHeuresticSeverity("app_5", HeuristicsResultDB.Severity.NONE, 10, 100);
        testFileHeuresticSeverity("app_6", HeuristicsResultDB.Severity.NONE, 100000, 100);
        testFileHeuresticSeverity("app_7", HeuristicsResultDB.Severity.LOW, 200000, 100);
    }

    private void computeHeuristic(String appId, String attempId, HashMap<FsAction, Integer> actions) {
        for (HashMap.Entry<FsAction, Integer> action : actions.entrySet()) {
            int incrementBy = action.getValue();
            while (incrementBy > 0) {
                heuristic.compute(appId, attempId, "", newFsEvent(action.getKey()));
                --incrementBy;
            }
        }
    }

    private void testFileHeuresticSeverity(String appId, int severity, int nbWrite, int nbDelete) {
        Mockito.doAnswer(invocationOnMock -> {
            HeuristicResult result = invocationOnMock.getArgumentAt(0, HeuristicResult.class);
            Assert.assertEquals(severity, result.getSeverity());
            Assert.assertEquals(severity, result.getScore());
            return null;
        }).when(db).createHeuristicResult(Matchers.any());
        HashMap<FsAction, Integer> actions = new HashMap<>();
        actions.put(FsAction.APPEND, 0);
        actions.put(FsAction.DELETE, nbDelete);
        actions.put(FsAction.READ, 0);
        actions.put(FsAction.RENAME, 0);
        actions.put(FsAction.WRITE, nbWrite);
        computeHeuristic(appId, "att_1", actions);
        heuristic.onAppCompleted(appId, "att_1");
        reset(db);
    }

    private DataAccessEventProtos.StateEvent newEndEvent() {
        return DataAccessEventProtos.StateEvent.newBuilder()
                .setState(State.END.toString())
                .build();
    }

    private DataAccessEventProtos.FsEvent newFsEvent(FsAction action) {
        return newFsEvent(action.toString());
    }

    private DataAccessEventProtos.FsEvent newFsEvent(String action) {
        return DataAccessEventProtos.FsEvent.newBuilder()
                .setDstPath("/tmp")
                .setSrcPath("/tmp")
                .setUri("/tmp")
                .setAction(action)
                .build();
    }


}
