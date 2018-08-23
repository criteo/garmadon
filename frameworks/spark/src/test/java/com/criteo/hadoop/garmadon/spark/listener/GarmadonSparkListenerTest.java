package com.criteo.hadoop.garmadon.spark.listener;

import com.criteo.hadoop.garmadon.event.proto.SparkEventProtos;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GarmadonSparkListenerTest {
    private List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

    private JavaSparkContext jsc;
    private SparkContext sc;

    private GarmadonSparkListener sparkListener;

    private Consumer<Object> eventHandler;

    @Before
    public void setUp() {
        eventHandler = mock(Consumer.class);
        SparkListernerConf.getInstance().setConsumer(eventHandler);

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

        verify(eventHandler, times(0)).accept(any(SparkEventProtos.StageEvent.class));
    }

    @Test
    public void SparkListener_should_get_insight_from_on_stage_complete_as_garmadon_listener_used() {
        assert (sc.listenerBus().listeners().contains(sparkListener));

        jsc.parallelize(data).count();
        jsc.close();

        verify(eventHandler, times(1)).accept(
                //any(StageEvent.class)
                anyObject()
        );
    }
}