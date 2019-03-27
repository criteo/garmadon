package com.criteo.hadoop.garmadon.flink;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos.JobEvent;
import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos.Property;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * Metric Reporter for Garmadon.
 *
 * <p>Variables in metrics scope will be sent as tags.
 */
public class GarmadonFlinkReporter implements MetricReporter, Scheduled {

    protected static final String HOST_VARIABLE = "<host>";
    protected static final String JOB_ID_VARIABLE = "<job_id>";
    protected static final String JOB_NAME_VARIABLE = "<job_name>";

    protected static final Set<String> JOB_MANAGER_WHITE_LIST = new HashSet<>();
    protected static final Set<String> JOB_WHITE_LIST = new HashSet<>();

    static {
        JOB_MANAGER_WHITE_LIST.add("taskSlotsAvailable");
        JOB_MANAGER_WHITE_LIST.add("taskSlotsTotal");
        JOB_MANAGER_WHITE_LIST.add("numRegisteredTaskManagers");
        JOB_MANAGER_WHITE_LIST.add("numRunningJobs");

        JOB_WHITE_LIST.add("totalNumberOfCheckpoints");
        JOB_WHITE_LIST.add("numberOfInProgressCheckpoints");
        JOB_WHITE_LIST.add("numberOfCompletedCheckpoints");
        JOB_WHITE_LIST.add("numberOfFailedCheckpoints");
        JOB_WHITE_LIST.add("lastCheckpointRestoreTimestamp");
        JOB_WHITE_LIST.add("lastCheckpointSize");
        JOB_WHITE_LIST.add("lastCheckpointDuration");
        JOB_WHITE_LIST.add("lastCheckpointAlignmentBuffered");
        JOB_WHITE_LIST.add("lastCheckpointExternalPath");
        JOB_WHITE_LIST.add("restartingTime");
        JOB_WHITE_LIST.add("downtime");
        JOB_WHITE_LIST.add("uptime");
        JOB_WHITE_LIST.add("fullRestarts");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonFlinkReporter.class);

    private final TriConsumer<Long, Header, Object> eventHandler;
    private final Header.SerializedHeader header;

    private String host;
    private Set<JobIdentifier> jobs = new HashSet<>();
    private Map<String, Gauge> gauges = new HashMap<>();


    public GarmadonFlinkReporter() {
        this(GarmadonFlinkConf.getInstance().getEventHandler(), GarmadonFlinkConf.getInstance().getHeader());
    }

    public GarmadonFlinkReporter(TriConsumer<Long, Header, Object> eventHandler, Header.SerializedHeader header) {
        this.eventHandler = eventHandler;
        this.header = header;
    }

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {
        host = metricGroup.getAllVariables().get(HOST_VARIABLE);

        String jobId = metricGroup.getAllVariables().get(JOB_ID_VARIABLE);
        String jobName = metricGroup.getAllVariables().get(JOB_NAME_VARIABLE);
        if (jobId != null) {
            jobs.add(new JobIdentifier(jobId, jobName));
        }

        String metricIdentifier = metricGroup.getMetricIdentifier(s);
        String metricName = getMetricName(metricIdentifier);

        // Filter according to white list
        if (!isMetricInWhiteList(metricName)) {
            LOGGER.warn("Metric [{}] is not in white list", metricName);
            return;
        }

        // There is only Gauges for now
        LOGGER.info("notifyOfAddedMetric [{}] with name [{}]", metricIdentifier, metricName);
        if (metric instanceof Gauge) {
            Gauge gauge = (Gauge) metric;
            gauges.put(metricName, gauge);
        }
    }

    private String getMetricName(String metricIdentifier) {
        return metricIdentifier.replaceFirst(host + ".", "")        // Remove the host from the metric name
            .replaceFirst("jobmanager.", "");    // Remove "jobmanager" from the metric name
    }

    private boolean isMetricInWhiteList(String metricName) {
        return JOB_MANAGER_WHITE_LIST.stream().anyMatch(metricName::endsWith) ||
            JOB_WHITE_LIST.stream().anyMatch(metricName::endsWith);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
        String metricIdentifier = metricGroup.getMetricIdentifier(s);
        String metricName = getMetricName(metricIdentifier);
        gauges.remove(metricName);
    }

    @Override
    public void report() {
        long currentTimeMillis = System.currentTimeMillis();
        reportJobManagerMetrics(currentTimeMillis);
        reportJobMetrics(currentTimeMillis);
    }

    private void reportJobManagerMetrics(long currentTimeMillis) {
        List<Property> jobManagerMetrics = createProperties(JOB_MANAGER_WHITE_LIST, Function.identity());

        FlinkEventProtos.JobManagerEvent.Builder builder = FlinkEventProtos.JobManagerEvent.newBuilder();
        builder.addAllMetrics(jobManagerMetrics);

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

    private List<Property> createProperties(Set<String> metrics, Function<String, String> nameAdapter) {
        List<Property> properties = new ArrayList<>();
        for (String metricName : metrics) {
            String jobMetricName = nameAdapter.apply(metricName);
            Gauge<Number> gauge = gauges.get(jobMetricName);
            if (gauge == null) {
                continue;
            }

            try {
                Property property = Property.newBuilder()
                    .setName(jobMetricName)
                    .setValue(gauge.getValue().longValue())
                    .build();
                properties.add(property);

            } catch (RuntimeException e) {
                LOGGER.error("Can not retrieve metric [{}]", jobMetricName, e);
            }
        }

        return properties;
    }

    private void reportJobMetrics(long currentTimeMillis) {
        jobs.forEach(jobIdentifier -> reportJobMetrics(currentTimeMillis, jobIdentifier));
    }

    private void reportJobMetrics(long currentTimeMillis, JobIdentifier jobIdentifier) {
        List<Property> metrics = createProperties(
            JOB_WHITE_LIST,
            metricName -> jobIdentifier.getJobName() + "." + metricName);

        JobEvent.Builder builder = JobEvent.newBuilder();
        builder.addAllMetrics(metrics);

        eventHandler.accept(currentTimeMillis, header, builder.build());
    }

}
