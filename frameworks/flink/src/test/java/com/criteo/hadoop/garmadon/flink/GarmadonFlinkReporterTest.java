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

  private TriConsumer<Long,Header, Object> handler = mock(TriConsumer.class);

  private final Map<String, String> jobManagerVariables = new HashMap<>();
  private final Map<String, String> jobVariables = new HashMap<>();

  private GarmadonFlinkReporter reporter = new GarmadonFlinkReporter(handler, DUMMY_HEADER.toSerializeHeader());

  @Before
  public void setUp() {
    jobManagerVariables.put(HOST_VARIABLE, "localhost");

    jobVariables.put(HOST_VARIABLE, HOST);
    jobVariables.put(JOB_ID_VARIABLE, JOB_ID);
    jobVariables.put(JOB_NAME_VARIABLE, JOB_NAME);
  }

  private List<FlinkEventProtos.Property> notifyOfAddedMetric(Map<String, String> variables, Set<String> metrics) {
    MetricGroup metricGroup = new SimpleMetricGroup(variables);

    return metrics.stream().map(metric -> {
      String metricName = metric.replaceFirst(HOST + ".", "").replaceFirst("jobmanager.", "");
      FlinkEventProtos.Property property = createProperty(metricName, random.nextLong());
      Gauge gauge = new SimpleGauge(property.getValue());
      reporter.notifyOfAddedMetric(gauge, metric, metricGroup);
      return property;
    }).collect(Collectors.toList());
  }

  private FlinkEventProtos.Property createProperty(String name, Long value) {
    return FlinkEventProtos.Property.newBuilder()
      .setName(name)
      .setValue(value)
      .build();
  }

  @Test
  public void testNotifyThenReport() {
    // GIVEN: notifyOfAddedMetric for metrics
    Set<String> jobManagerMetricNames = JOB_MANAGER_WHITE_LIST.stream().map(name -> HOST + ".jobmanager." + name).collect(toSet());
    List<FlinkEventProtos.Property> jobManagerProperties = notifyOfAddedMetric(jobManagerVariables, jobManagerMetricNames);

    Set<String> jobMetricNames = JOB_WHITE_LIST.stream().map(name -> HOST + ".jobmanager." + JOB_NAME + "." + name).collect(toSet());
    List<FlinkEventProtos.Property> jobProperties = notifyOfAddedMetric(jobVariables, jobMetricNames);

    // WHEN: report
    reporter.report();

    // THEN: handler is called twice
    ArgumentCaptor<Object> eventArgumentCaptor = ArgumentCaptor.forClass(Object.class);
    verify(handler, times(2)).accept(any(Long.class), any(Header.class), eventArgumentCaptor.capture());

    List<Object> capturedValues = eventArgumentCaptor.getAllValues();
    assertThat(capturedValues).hasSize(2);

    // THEN: handler is called for JobManagerEvent with expected Metrics
    FlinkEventProtos.JobManagerEvent jobManagerEvent = (FlinkEventProtos.JobManagerEvent) capturedValues.get(0);
    assertThat(jobManagerEvent.getMetricsList()).containsAll(jobManagerProperties);

    // THEN: handler is called for JobEvent with expected Metrics
    FlinkEventProtos.JobEvent jobEvent = (FlinkEventProtos.JobEvent) capturedValues.get(1);
    assertThat(jobEvent.getMetricsList()).containsAll(jobProperties);
  }
}
