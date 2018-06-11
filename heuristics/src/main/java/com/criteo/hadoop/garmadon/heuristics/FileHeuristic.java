package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FileHeuristic {

    private static Logger LOGGER = LoggerFactory.getLogger(FileHeuristic.class);

    final Counters deleted = new Counters("Files deleted");
    final Counters read = new Counters("Files read");
    final Counters written = new Counters("Files written");
    final Counters renamed = new Counters("Files renamed");

    private final HeuristicsResultDB db;
    private final Map<String, Set<String>> containersPerApp;

    public FileHeuristic(HeuristicsResultDB db) {
        this.db = db;
        this.containersPerApp = new HashMap<>();
    }

    static class Counters {

        private final HashMap<String, Counter> counters = new HashMap<>();
        private final String name;

        Counters(String name) {
            this.name = name;
        }

        Counter forApp(String applicationId) {
            return counters.computeIfAbsent(applicationId, s -> new Counter());
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

    public void compute(String applicationId, String containerId, DataAccessEventProtos.FsEvent fsEvent) {
        containersFor(applicationId).add(containerId);
        try {
            FsEvent.Action action = FsEvent.Action.valueOf(fsEvent.getAction());
            switch (action) {
                case DELETE:
                    deleted.forApp(applicationId).increment();
                    break;
                case READ:
                    read.forApp(applicationId).increment();
                    break;
                case WRITE:
                    written.forApp(applicationId).increment();
                    break;
                case RENAME:
                    renamed.forApp(applicationId).increment();
                    break;
            }
        } catch(IllegalArgumentException ex) {
            LOGGER.warn("received an unexpected FsEvent.Action {}", ex.getMessage());
        }
    }

    private Set<String> containersFor(String applicationId) {
        return containersPerApp.computeIfAbsent(applicationId, s -> new HashSet<>());
    }

    public void compute(String applicationId, String containerId, DataAccessEventProtos.StateEvent stateEvent){
        if("END".equals(stateEvent.getState())){
            Set<String> appContainers = containersFor(applicationId);
            if(appContainers.size() == 0) return; //already emptied
            appContainers.remove(containerId);
            if(appContainers.size() == 0){
                //TODO compute severity based on number of deleted, renamed, read, written...
                HeuristicResult result = new HeuristicResult(applicationId, "", FileHeuristic.class, HeuristicsResultDB.Severity.NONE, HeuristicsResultDB.Severity.NONE);
                addDetail(result, deleted, applicationId);
                addDetail(result, read, applicationId);
                addDetail(result, written, applicationId);
                addDetail(result, renamed, applicationId);
                db.createHeuristicResult(result);
            }
        }
    }

    private void addDetail(HeuristicResult result, Counters counters, String applicationId) {
        result.addDetail(counters.name, Integer.toString(counters.forApp(applicationId).getCount()));
    }

}

