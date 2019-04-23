package com.criteo.hadoop.garmadon.flink;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.stream.Collectors;

import static com.criteo.hadoop.garmadon.flink.GarmadonFlinkReporter.*;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class GarmadonFlinkReporterTest {

    private static final Random random = new Random();

    private static final Header DUMMY_HEADER = new Header("id", "appId", "appAttemptId",
            "appName", "user", "container", "hostname",
            Collections.singletonList("tag"), "pid", "framework", "component",
            "executorId", "mainClass");

    private static final String HOST = "localhost";
    private static final String JOB_ID = "SomeJobId";
    private static final String JOB_NAME = "SomeJobName";
    private static final String TM_ID = "container_01";
    private static final String TASK_ID = "SomeTaskId";
    private static final String TASK_NAME = "SomeTaskName";

    private TriConsumer<Long, Header, Object> handler = mock(TriConsumer.class);

    private final Map<String, String> jobManagerVariables = new HashMap<>();
    private final Map<String, String> jobVariables = new HashMap<>();

    private final Map<String, String> taskManagerVariables = new HashMap<>();
    private final Map<String, String> taskVariables = new HashMap<>();

    private GarmadonFlinkReporter reporter;

    @Before
    public void setUp() {
        jobManagerVariables.put(HOST_VARIABLE, HOST);
        jobManagerVariables.put("<other>", "jobmanager");

        jobVariables.putAll(jobManagerVariables);
        jobVariables.put("<other>", "jobmanager");
        jobVariables.put(JOB_ID_VARIABLE, JOB_ID);
        jobVariables.put(JOB_NAME_VARIABLE, JOB_NAME);

        taskManagerVariables.put(HOST_VARIABLE, HOST);
        taskManagerVariables.put("<other>", "taskmanager");
        taskManagerVariables.put(TASK_ID_VARIABLE, TM_ID);

        taskVariables.putAll(jobVariables);
        taskVariables.putAll(taskManagerVariables);
        taskVariables.put(TASK_ID_VARIABLE, TASK_ID);
        taskVariables.put(TASK_NAME_VARIABLE, TASK_NAME);
        taskVariables.put(TASK_ATTEMPT_NUM_VARIABLE, "0");
        taskVariables.put(SUBTASK_INDEX_VARIABLE, "0");

        reporter = new GarmadonFlinkReporter(handler, DUMMY_HEADER.toSerializeHeader());
    }

    private Long notifyOfAddedMetric(Map<String, String> variables, String metric) {
        MetricGroup metricGroup = new SimpleMetricGroup(variables);
        Long value = random.nextLong();
        Gauge gauge = new SimpleGauge(value);
        reporter.notifyOfAddedMetric(gauge, metric, metricGroup);
        return value;
    }

    @Test
    public void testNotifyJobManagerThenReport() {
        Long valueNumRegisteredTaskManagers = notifyOfAddedMetric(jobManagerVariables, HOST + ".jobmanager.numRegisteredTaskManagers");
        Long valueNumberOfFailedCheckpoints = notifyOfAddedMetric(jobVariables, HOST + ".jobmanager." + JOB_NAME + ".numberOfFailedCheckpoints");

        // WHEN: report
        reporter.report();

        // THEN: handler is called twice
        ArgumentCaptor<Object> eventArgumentCaptor = ArgumentCaptor.forClass(Object.class);
        verify(handler, times(2)).accept(any(Long.class), any(Header.class), eventArgumentCaptor.capture());

        List<Object> capturedValues = eventArgumentCaptor.getAllValues();
        assertThat(capturedValues).hasSize(2);

        FlinkEventProtos.JobManagerEvent jobManagerEvent = (FlinkEventProtos.JobManagerEvent) capturedValues.get(0);
        assertThat(jobManagerEvent.getNumRegisteredTaskManagers() == valueNumRegisteredTaskManagers);

        FlinkEventProtos.JobEvent jobEvent = (FlinkEventProtos.JobEvent) capturedValues.get(1);
        assertThat(jobEvent.getNumberOfFailedCheckpoints() == valueNumberOfFailedCheckpoints);
    }

    @Test
    public void testNotifyTaskManagerThenReport() {
        Long valueNetworkTotalMemorySegments = notifyOfAddedMetric(taskManagerVariables, HOST + ".taskmanager." + TM_ID + ".Status.Network.TotalMemorySegments");
        Long valueNumBytesOutPerSecond = notifyOfAddedMetric(taskVariables, HOST + ".taskmanager." + JOB_NAME + "." + TASK_NAME + ".0." + ".numBytesOutPerSecond");

        // WHEN: report
        reporter.report();

        // THEN: handler is called twice
        ArgumentCaptor<Object> eventArgumentCaptor = ArgumentCaptor.forClass(Object.class);
        verify(handler, times(2)).accept(any(Long.class), any(Header.class), eventArgumentCaptor.capture());

        List<Object> capturedValues = eventArgumentCaptor.getAllValues();
        assertThat(capturedValues).hasSize(2);

        FlinkEventProtos.TaskManagerEvent taskManagerEvent = (FlinkEventProtos.TaskManagerEvent) capturedValues.get(0);
        assertThat(taskManagerEvent.getNetworkTotalMemorySegments() == valueNetworkTotalMemorySegments);

        FlinkEventProtos.TaskEvent taskEvent = (FlinkEventProtos.TaskEvent) capturedValues.get(1);
        assertThat(taskEvent.getNumBytesOutPerSecond() == valueNumBytesOutPerSecond);
    }
}
