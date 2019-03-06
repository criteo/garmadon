package com.criteo.hadoop.garmadon.agent.tracers.hadoop.nodemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class ContainerResourceMonitoringTracer {

    private static TriConsumer<Long, Header, Object> eventHandler;
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerResourceMonitoringTracer.class);

    protected ContainerResourceMonitoringTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header baseHeader, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {

        initEventHandler((timestamp, headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(timestamp, header, event);
        });

        new MemorySizeTracer().installOn(instrumentation);
        new VcoreUsageTracer().installOn(instrumentation);
    }

    public static void initEventHandler(TriConsumer<Long, Header, Object> eventHandler) {
        ContainerResourceMonitoringTracer.eventHandler = eventHandler;
    }

    public static class MemorySizeTracer extends MethodTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("isProcessTreeOverLimit").and(takesArguments(String.class, long.class, long.class, long.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(MemorySizeTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(0) String containerID, @Argument(1) long currentMemUsage,
                                     @Argument(3) long limit) throws Exception {
            try {

                ContainerId cID = ConverterUtils.toContainerId(containerID);
                ApplicationAttemptId applicationAttemptId = cID.getApplicationAttemptId();
                String applicationId = applicationAttemptId.getApplicationId().toString();
                String attemptId = applicationAttemptId.toString();

                Header header = Header.newBuilder()
                        .withId(applicationId)
                        .withApplicationID(applicationId)
                        .withAttemptID(attemptId)
                        .withContainerID(containerID)
                        .build();

                long memUsage = (currentMemUsage > 0) ? currentMemUsage : 0;

                ContainerEventProtos.ContainerResourceEvent event = ContainerEventProtos.ContainerResourceEvent.newBuilder()
                        .setType(ContainerType.MEMORY.name())
                        .setValue(memUsage)
                        .setLimit(limit)
                        .build();
                eventHandler.accept(System.currentTimeMillis(), header, event);
            } catch (Exception ignored) {
            }
        }
    }

    public static class VcoreUsageTracer extends MethodTracer {

        /**
         * We have to load the class ContainerMetrics after instrumenting it
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
                } catch (Exception ignored) {
                }
            }
        }

        static Field getField() {
            return SingletonHolder.field;
        }


        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("recordCpuUsage").and(takesArguments(int.class, int.class));
        }

        @Override
        protected Implementation newImplementation() {
            return to(VcoreUsageTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@This Object containerMetrics,
                                     @Argument(1) int milliVcoresUsed) throws Exception {
            try {
                if (getField() != null) {
                    ContainerId cID = (ContainerId) getField().get(containerMetrics);
                    ApplicationAttemptId applicationAttemptId = cID.getApplicationAttemptId();
                    String applicationId = applicationAttemptId.getApplicationId().toString();
                    String attemptId = applicationAttemptId.toString();

                    float cpuVcoreUsed = (milliVcoresUsed > 0) ? (float) milliVcoresUsed / 1000 : 0f;
                    int cpuVcoreLimit = ((ContainerMetrics) containerMetrics).cpuVcoreLimit.value();

                    Header header = Header.newBuilder()
                            .withId(applicationId)
                            .withApplicationID(applicationId)
                            .withAttemptID(attemptId)
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
            } catch (Exception ignored) {
            }
        }
    }
}
