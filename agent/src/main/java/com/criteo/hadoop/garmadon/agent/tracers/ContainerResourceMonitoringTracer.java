package com.criteo.hadoop.garmadon.agent.tracers;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.ContainerType;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.lang.instrument.Instrumentation;
import java.util.function.BiConsumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class ContainerResourceMonitoringTracer {

    private static BiConsumer<Header, Object> eventHandler;

    public static void setup(Header.BaseHeader baseHeader, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {

        initEventHandler((headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(header, event);
        });

        new MemorySizeTracer().installOn(instrumentation);
    }

    public static void initEventHandler(BiConsumer<Header, Object> eventHandler) {
        ContainerResourceMonitoringTracer.eventHandler = eventHandler;
    }

    public static class MemorySizeTracer extends MethodTracer {

        @Override
        ElementMatcher<? super TypeDescription> typeMatcher() {
            return nameStartsWith("org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl");
        }

        @Override
        ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("isProcessTreeOverLimit").and(takesArguments(String.class, long.class, long.class, long.class));
        }

        @Override
        Implementation newImplementation() {
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
                        .withAppAttemptID(attemptId)
                        .withContainerID(containerID)
                        .build();

                ContainerEventProtos.ContainerResourceEvent event = ContainerEventProtos.ContainerResourceEvent.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setType(ContainerType.MEMORY.name())
                        .setValueFloat(currentMemUsage)
                        .setLimit(limit)
                        .build();
                eventHandler.accept(header, event);
            } catch (Exception ignore) {
            }
        }
    }
}
