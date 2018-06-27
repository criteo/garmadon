package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;

public class FileHeuristic implements Heuristic {

    private static Logger LOGGER = LoggerFactory.getLogger(FileHeuristic.class);

    final Counters deleted = new Counters("Files deleted");
    final Counters read = new Counters("Files read");
    final Counters written = new Counters("Files written");
    final Counters renamed = new Counters("Files renamed");

    private final HeuristicsResultDB db;

    public FileHeuristic(HeuristicsResultDB db) {
        this.db = db;
    }

    public void compute(String applicationId, String attemptId, String containerId, DataAccessEventProtos.FsEvent fsEvent) {
        try {
            FsEvent.Action action = FsEvent.Action.valueOf(fsEvent.getAction());
            switch (action) {
                case DELETE:
                    deleted.forApp(applicationId, attemptId).increment();
                    break;
                case READ:
                    read.forApp(applicationId, attemptId).increment();
                    break;
                case WRITE:
                    written.forApp(applicationId, attemptId).increment();
                    break;
                case RENAME:
                    renamed.forApp(applicationId, attemptId).increment();
                    break;
            }
        } catch (IllegalArgumentException ex) {
            LOGGER.warn("received an unexpected FsEvent.Action {}", ex.getMessage());
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {

    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        //TODO compute severity based on number of deleted, renamed, read, written...
        HeuristicResult result = new HeuristicResult(applicationId, attemptId, FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
        addDetail(result, deleted, applicationId, attemptId);
        addDetail(result, read, applicationId, attemptId);
        addDetail(result, written, applicationId, attemptId);
        addDetail(result, renamed, applicationId, attemptId);
        db.createHeuristicResult(result);
    }

    private void addDetail(HeuristicResult result, Counters counters, String applicationId, String attemptId) {
        result.addDetail(counters.name, Integer.toString(counters.forApp(applicationId, attemptId).getCount()));
    }

    static class Counters {

        private final HashMap<String, Counter> counters = new HashMap<>();
        private final String name;

        Counters(String name) {
            this.name = name;
        }

        Counter forApp(String applicationId, String attemptId) {
            return counters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId), s -> new Counter());
        }

        static class Counter {

            private int count = 0;

            int getCount() {
                return count;
            }

            private void increment() {
                count++;
            }
        }
    }

}

