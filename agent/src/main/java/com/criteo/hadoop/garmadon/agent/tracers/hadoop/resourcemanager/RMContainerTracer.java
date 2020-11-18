package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.tracers.MethodTracer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
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
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;


public class RMContainerTracer {
    private static TriConsumer<Long, Header, Object> eventHandler;

    protected RMContainerTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header baseHeader, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        initEventHandler((timestamp, headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(timestamp, header, event);
        });
        // Get Events of containers
        new RMContainerImplTracer().installOn(instrumentation);
    }

    public static TriConsumer<Long, Header, Object> getEventHandler() {
        return eventHandler;
    }

    public static void initEventHandler(TriConsumer<Long, Header, Object> eventHandler) {
        RMContainerTracer.eventHandler = eventHandler;
    }

    public static class RMContainerImplTracer extends MethodTracer {

        private static class SingletonHolder {
            private static Field field;

            static {
                try {
                    field = RMContainerImpl.class.getDeclaredField("finishedStatus");
                    field.setAccessible(true);
                } catch (Throwable ignored) {
                }
            }
        }

        static Field getField() {
            return RMContainerTracer.RMContainerImplTracer.SingletonHolder.field;
        }


        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return named("org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> methodMatcher() {
            return named("handle");
        }

        @Override
        protected Implementation newImplementation() {
            return SuperMethodCall.INSTANCE.andThen(to(RMContainerImplTracer.class));
        }

        public static void intercept(@This Object rmContainerImpl,
                                     @Argument(0) RMContainerEvent rmContainerEvent) {
            try {
                ContainerId cID = rmContainerEvent.getContainerId();
                ApplicationAttemptId applicationAttemptId = cID.getApplicationAttemptId();
                String applicationId = applicationAttemptId.getApplicationId().toString();
                String attemptId = applicationAttemptId.toString();

                Header header = Header.newBuilder()
                    .withId(applicationId)
                    .withApplicationID(applicationId)
                    .withAttemptID(attemptId)
                    .withContainerID(cID.toString())
                    .build();

                RMContainerImpl rmc = (RMContainerImpl) rmContainerImpl;

                ResourceManagerEventProtos.ContainerEvent.Builder eventBuilder = ResourceManagerEventProtos.ContainerEvent.newBuilder()
                    .setType(rmContainerEvent.getType().name())
                    .setState(rmc.getState().name())
                    .setStartTime(rmc.getCreationTime())
                    .setLogUrl(rmc.getLogURL());

                if (rmc.getContainer() != null && rmc.getContainer().getResource() != null) {
                    eventBuilder.setVcoresReserved(rmc.getContainer().getResource().getVirtualCores());
                    eventBuilder.setMemoryReserved(rmc.getContainer().getResource().getMemory());
                }

                if (rmc.getAllocatedNode() != null) {
                    eventBuilder.setContainerHostname(rmc.getAllocatedNode().getHost());
                }

                ContainerStatus containerStatus = (ContainerStatus) getField().get(rmContainerImpl);
                if (containerStatus != null) {
                    eventBuilder
                        .setIsFinished(true)
                        .setExitStatus(rmc.getContainerExitStatus())
                        .setReason(rmc.getDiagnosticsInfo())
                        .setFinishTime(rmc.getFinishTime());
                }

                eventHandler.accept(System.currentTimeMillis(), header, eventBuilder.build());
            } catch (Throwable ignored) {
            }
        }
    }
}
