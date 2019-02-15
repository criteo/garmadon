package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class FileHeuristic implements Heuristic {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileHeuristic.class);

    protected final Counters deleted = new Counters("Files deleted");
    protected final Counters read = new Counters("Files read");
    protected final Counters written = new Counters("Files written");
    protected final Counters renamed = new Counters("Files renamed");
    protected final Counters append = new Counters("Files appended");

    private final HeuristicsResultDB db;
    private final int maxCreatedFiles;

    public FileHeuristic(HeuristicsResultDB db, int maxCreatedFiles) {
        this.db = db;
        this.maxCreatedFiles = maxCreatedFiles;
    }

    public void compute(String applicationId, String attemptId, String containerId, DataAccessEventProtos.FsEvent fsEvent) {
        try {
            FsAction action = FsAction.valueOf(fsEvent.getAction());
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
                case APPEND:
                    append.forApp(applicationId, attemptId).increment();
                    break;
                default:
                    throw new IllegalArgumentException("Received a non managed FsEvent.Action " + action.name());
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
        int created = written.forApp(applicationId, attemptId).getCount();
        int severity;
        if (created > maxCreatedFiles) severity = HeuristicsResultDB.Severity.SEVERE;
        else {
            if (created > maxCreatedFiles / 2) severity = HeuristicsResultDB.Severity.MODERATE;
            else {
                if (created > maxCreatedFiles / 10) severity = HeuristicsResultDB.Severity.LOW;
                else severity = HeuristicsResultDB.Severity.NONE;
            }
        }

        HeuristicResult result = new HeuristicResult(applicationId, attemptId, FileHeuristic.class, severity, severity);
        addDetail(result, deleted, applicationId, attemptId);
        addDetail(result, read, applicationId, attemptId);
        addDetail(result, written, applicationId, attemptId);
        addDetail(result, renamed, applicationId, attemptId);
        addDetail(result, append, applicationId, attemptId);
        db.createHeuristicResult(result);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("FileHeuristic");
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

            private int count;

            int getCount() {
                return count;
            }

            private void increment() {
                count++;
            }
        }
    }

}

