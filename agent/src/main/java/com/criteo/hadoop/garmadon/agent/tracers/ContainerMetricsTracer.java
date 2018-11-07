package com.criteo.hadoop.garmadon.agent.tracers;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.ContainerType;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;


// Require yarn.nodemanager.container-metrics.enable configuration set to true
public class ContainerMetricsTracer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerMetricsTracer.class);
    private static TriConsumer<Long, Header, Object> eventHandler;

    public static void setup(Header baseHeader, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        initEventHandler((timestamp, headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(timestamp, header, event);
        });
        new VcoreUsageTracer().installOn(instrumentation);
    }

    public static void initEventHandler(TriConsumer<Long, Header, Object> eventHandler) {
        ContainerMetricsTracer.eventHandler = eventHandler;
    }

    public static class VcoreUsageTracer extends MethodTracer {

        /**
         * We have to load the class after instrumenting it
         * If we set it directly in a static in VcoreUsageTracer it will load the class
         * before instrumenting it
         * With Singleton mechanism we ensure load of class only when accesing to getField method
         */
        private static class SingletonHolder {
            private static Field field;

            static {
                try {
                    field = ContainerMetrics.class.getDeclaredField("containerId");
                    field.setAccessible(true);
                } catch (Exception ignore) {
                }
            }
        }

        public static Field getField() {
            return SingletonHolder.field;
        }


        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics");
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("recordCpuUsage").and(takesArguments(int.class, int.class));
        }

        @Override
        Implementation newImplementation() {
            return to(VcoreUsageTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@This Object o,
                                     @Argument(1) int milliVcoresUsed) throws Exception {
            try {
                if (getField() != null) {
                    ContainerId cID = (ContainerId) getField().get(o);
                    ApplicationAttemptId applicationAttemptId = cID.getApplicationAttemptId();
                    String applicationId = applicationAttemptId.getApplicationId().toString();
                    String attemptId = applicationAttemptId.toString();

                    float cpuVcoreUsed = (float) milliVcoresUsed / 1000;
                    int cpuVcoreLimit = ((ContainerMetrics) o).cpuVcoreLimit.value();

                    Header header = Header.newBuilder()
                            .withId(applicationId)
                            .withApplicationID(applicationId)
                            .withAppAttemptID(attemptId)
                            .withContainerID(cID.toString())
                            .build();

                    ContainerEventProtos.ContainerResourceEvent event = ContainerEventProtos.ContainerResourceEvent.newBuilder()
                            .setType(ContainerType.VCORE.name())
                            .setValue(cpuVcoreUsed)
                            .setLimit(cpuVcoreLimit)
                            .build();
                    eventHandler.accept(System.currentTimeMillis(), header, event);
                } else {
                    LOGGER.warn("ContainerMetrics class does not have containerId field");
                }
            } catch (Exception ignore) {
            }
        }
    }
}
