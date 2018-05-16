package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.schema.events.ContainerResourceEvent;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.BiConsumer;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class ContainerResourceMonitoringModule implements GarmadonAgentModule {

    private final Header.BaseHeader baseHeader;

    public ContainerResourceMonitoringModule() {
        String host = "";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignored) {
        }
        this.baseHeader = Header.newBuilder()
                .withHostname(host)
                .withTag(Header.Tag.NODEMANAGER.name())
                .buildBaseHeader();
    }

    @Override
    public void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        new MemorySizeTracer((headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(header, event);
        }).installOn(instrumentation);
    }

    public static class MemorySizeTracer extends MethodTracer {

        private final BiConsumer<Header, Object> eventHandler;

        public MemorySizeTracer(BiConsumer<Header, Object> eventHandler) {
            this.eventHandler = eventHandler;
        }

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
            return to(this).andThen(SuperMethodCall.INSTANCE);
        }

        public void intercept(@Argument(0) String containerID, @Argument(1) long currentMemUsage,
                              @Argument(3) long limit) throws Exception {
            try {
                ContainerId cID = ConverterUtils.toContainerId(containerID);
                ApplicationAttemptId applicationAttemptId = cID.getApplicationAttemptId();
                String applicationId = applicationAttemptId.getApplicationId().toString();
                String attemptId = applicationAttemptId.toString();

                Header header = Header.newBuilder()
                        .withApplicationID(applicationId)
                        .withAppAttemptID(attemptId)
                        .withContainerID(containerID)
                        .build();

                ContainerResourceEvent event = new ContainerResourceEvent(System.currentTimeMillis(), ContainerResourceEvent.Type.MEMORY, currentMemUsage, limit);
                eventHandler.accept(header, event);
            } catch (Exception ignore) {
            }
        }
    }
}
