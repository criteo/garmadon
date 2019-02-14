package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

public class GarmadonSparkListenerIntegrationTest {
    private List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

    private JavaSparkContext jsc;
    private SparkContext sc;

    private GarmadonSparkListener sparkListener;

    private TriConsumer<Long, Header, Object> eventHandler;
    private Header.SerializedHeader header;

    @Before
    public void setUp() {
        eventHandler = mock(TriConsumer.class);
        header = Header.newBuilder()
                .withId("id")
                .addTag(Header.Tag.STANDALONE.name())
                .withHostname("host")
                .withUser("user")
                .withPid("pid")
                .buildSerializedHeader();

        SparkListernerConf.getInstance().setConsumer(eventHandler);
        SparkListernerConf.getInstance().setHeader(header);

        jsc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("TestGarmadonListener")
                        .setMaster("local[1]")
                        .set("spark.driver.allowMultipleContexts", "true")
        );
        sc = jsc.sc();

        sparkListener = new GarmadonSparkListener();
        sc.addSparkListener(sparkListener);
    }

    @Test
    public void SparkListener_should_not_get_insight_from_on_stage_complete_as_garmadon_listener_is_removed() {
        sc.removeSparkListener(sparkListener);

        jsc.parallelize(data).count();
        jsc.close();

        verify(eventHandler, times(0)).accept(any(Long.class), any(Header.class), any(SparkEventProtos.StageEvent.class));
    }

    @Test
    public void SparkListener_should_get_insight_from_on_stage_complete_as_garmadon_listener_used() {
        assert (sc.listenerBus().listeners().contains(sparkListener));

        jsc.parallelize(data).count();
        jsc.close();

        verify(eventHandler, times(2)).accept(isA(Long.class), isA(Header.class), isA(SparkEventProtos.StageStateEvent.class));
        verify(eventHandler).accept(isA(Long.class), isA(Header.class), isA(SparkEventProtos.StageEvent.class));
    }
}