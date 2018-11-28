package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader that pushes events to elastic search
 */
public final class ElasticSearchListener implements BulkProcessor.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchListener.class);

    private static Counter.Child numberOfEventInError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_event_in_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);
    private static Counter.Child numberOfOffsetCommitError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_offset_commit_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);
    private static Summary.Child latencyIndexingEvents = PrometheusHttpConsumerMetrics.LATENCY_INDEXING_TO_ES.labels("latency_indexing_events",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOGGER.debug("Executing Bulk[{}] with {} requests of {} Bytes", executionId,
                request.numberOfActions(),
                request.estimatedSizeInBytes());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            LOGGER.error("Bulk[{}] executed with failures", executionId);
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    LOGGER.error("Failed on {} due to {}", item.getId(), item.getFailureMessage());
                    numberOfEventInError.inc();
                }
            }
        } else {
            LOGGER.info("Successfully completed Bulk[{}] in {} ms", executionId, response.getTook().getMillis());
            latencyIndexingEvents.observe(response.getTook().getMillis());
        }
        CommittableOffset<String, byte[]> lastOffset = (CommittableOffset<String, byte[]>) request.payloads().get(request.payloads().size() - 1);
        lastOffset
                .commitAsync()
                .whenComplete((topicPartitionOffset, exception) -> {
                    if (exception != null) {
                        LOGGER.warn("Could not commit kafka offset {}|{}", lastOffset.getOffset(), lastOffset.getPartition());
                        numberOfOffsetCommitError.inc();
                    } else {
                        LOGGER.info("Committed kafka offset {}|{}", topicPartitionOffset.getOffset(), topicPartitionOffset.getPartition());
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute Bulk[{}]", executionId, failure);
        numberOfEventInError.inc(request.requests().size());
    }
}
