package com.criteo.hadoop.garmadon.heuristics.flink;

import com.criteo.hadoop.garmadon.heuristics.HeuristicResult;
import com.criteo.hadoop.garmadon.heuristics.HeuristicsResultDB;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Random;
import java.util.UUID;

import static com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos.*;
import static com.criteo.hadoop.garmadon.heuristics.flink.FlinkCheckpointDuration.FIFTEEN_MINUTES_IN_MS;
import static com.criteo.hadoop.garmadon.heuristics.flink.FlinkCheckpointDuration.PROPERTY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class FlinkCheckpointDurationTest {

  private static final Random R = new Random();
  private static final String JOB_ID = "job_id";
  private static final String JOB_NAME = "job_name";

  private HeuristicsResultDB db = mock(HeuristicsResultDB.class);

  private FlinkCheckpointDuration heuristic = new FlinkCheckpointDuration(db);

  @Test
  public void exportHeuristicsResults_do_nothing_when_no_events_received() {
    heuristic.exportHeuristicsResults();
    verify(db, never()).createHeuristicResult(any());
  }

  @Test
  public void onAppCompleted_do_nothing_when_no_events_received() {
    heuristic.onAppCompleted(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    verify(db, never()).createHeuristicResult(any());
  }

  @Test
  public void exportHeuristicsResults_do_nothing_when_JobManagerEvent_received() {
    JobManagerEvent event = JobManagerEvent.newBuilder()
      .setTaskSlotsTotal(R.nextLong())
      .build();
    heuristic.process(R.nextLong(), UUID.randomUUID().toString(), event);

    heuristic.exportHeuristicsResults();

    verify(db, never()).createHeuristicResult(any());
  }

  @Test
  public void exportHeuristicsResults_createHeuristicResult_with_NONE_severity_when_JobEvent_received() {
    // GIVEN: JobEvent with less than 15min has been received
    long duration = FIFTEEN_MINUTES_IN_MS - 1;
    JobEvent event = createJobEvent(duration);
    String applicationId = UUID.randomUUID().toString();
    heuristic.process(R.nextLong(), applicationId, event);

    // WHEN: exportHeuristicsResults
    heuristic.exportHeuristicsResults();

    // THEN: createHeuristicResult has been called
    ArgumentCaptor<HeuristicResult> argumentCaptor = ArgumentCaptor.forClass(HeuristicResult.class);
    verify(db).createHeuristicResult(argumentCaptor.capture());

    checkHeuristicResult(argumentCaptor.getValue(), applicationId, duration, HeuristicsResultDB.Severity.NONE);
  }

  private JobEvent createJobEvent(long duration) {
    return JobEvent.newBuilder()
      .setJobId(JOB_ID)
      .setJobName(JOB_NAME)
      .setLastCheckpointDuration(duration)
      .build();
  }

  private void checkHeuristicResult(HeuristicResult result, String applicationId, long duration, int expectedSeverity) {
    assertThat(result.getAppId()).isEqualTo(applicationId);
    assertThat(result.getHeuristicClass()).isEqualTo(FlinkCheckpointDuration.class);
    assertThat(result.getScore()).isEqualTo(expectedSeverity);
    assertThat(result.getSeverity()).isEqualTo(expectedSeverity);
    assertThat(result.getDetailCount()).isEqualTo(1);
    assertThat(result.getDetail(0)).isEqualTo(new HeuristicResult.HeuristicResultDetail(PROPERTY_NAME, String.valueOf(duration), null));
  }

  @Test
  public void exportHeuristicsResults_createHeuristicResult_with_SEVERE_severity_when_JobEvent_received() {
    // GIVEN: JobEvent with more than 15min has been received
    long duration = FIFTEEN_MINUTES_IN_MS + 1;
    JobEvent event = createJobEvent(duration);
    String applicationId = UUID.randomUUID().toString();
    heuristic.process(R.nextLong(), applicationId, event);

    // WHEN: exportHeuristicsResults
    heuristic.exportHeuristicsResults();

    // THEN: createHeuristicResult has been called
    ArgumentCaptor<HeuristicResult> argumentCaptor = ArgumentCaptor.forClass(HeuristicResult.class);
    verify(db).createHeuristicResult(argumentCaptor.capture());

    checkHeuristicResult(argumentCaptor.getValue(), applicationId, duration, HeuristicsResultDB.Severity.SEVERE);
  }

  @Test
  public void exportHeuristicsResults_createHeuristicResult_with_SEVERE_severity_when_2_JobEvent_received() {
    String applicationId = UUID.randomUUID().toString();

    // GIVEN: JobEvent with more than 15min has been received
    long durationMore15 = FIFTEEN_MINUTES_IN_MS + 1;
    JobEvent eventMore15 = createJobEvent(durationMore15);
    heuristic.process(R.nextLong(), applicationId, eventMore15);

    // GIVEN: JobEvent with less than 15min has been received
    long durationLess15 = FIFTEEN_MINUTES_IN_MS - 1;
    JobEvent eventLess15 = createJobEvent(durationLess15);
    heuristic.process(R.nextLong(), applicationId, eventLess15);

    // WHEN: exportHeuristicsResults
    heuristic.exportHeuristicsResults();

    // THEN: createHeuristicResult has been called
    ArgumentCaptor<HeuristicResult> argumentCaptor = ArgumentCaptor.forClass(HeuristicResult.class);
    verify(db).createHeuristicResult(argumentCaptor.capture());

    // Severity is SEVERE
    checkHeuristicResult(argumentCaptor.getValue(), applicationId, durationMore15, HeuristicsResultDB.Severity.SEVERE);
  }

  @Test
  public void onAppCompleted_createHeuristicResult_with_NONE_severity_when_JobEvent_received() {
    // GIVEN: JobEvent with less than 15min has been received
    long duration = FIFTEEN_MINUTES_IN_MS - 1;
    JobEvent event = createJobEvent(duration);
    String applicationId = UUID.randomUUID().toString();
    heuristic.process(R.nextLong(), applicationId, event);

    // WHEN: onAppCompleted
    heuristic.onAppCompleted(applicationId, UUID.randomUUID().toString());

    // THEN: createHeuristicResult has been called
    ArgumentCaptor<HeuristicResult> argumentCaptor = ArgumentCaptor.forClass(HeuristicResult.class);
    verify(db).createHeuristicResult(argumentCaptor.capture());

    checkHeuristicResult(argumentCaptor.getValue(), applicationId, duration, HeuristicsResultDB.Severity.NONE);
  }
}
