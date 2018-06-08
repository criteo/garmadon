package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.events.FsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class FileHeuristic {

    private static Logger LOGGER = LoggerFactory.getLogger(FileHeuristic.class);

    final Counters deleted = new Counters();
    final Counters read = new Counters();
    final Counters written = new Counters();
    final Counters renamed = new Counters();

    static class Counters {

        private HashMap<String, Counter> counters = new HashMap<>();

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

}

