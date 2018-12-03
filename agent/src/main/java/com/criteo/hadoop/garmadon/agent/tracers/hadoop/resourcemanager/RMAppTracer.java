package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.agent.tracers.ConstructorTracer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.any;
import static net.bytebuddy.matcher.ElementMatchers.named;


public class RMAppTracer {
    private static TriConsumer<Long, Header, Object> eventHandler;

    protected RMAppTracer() {
        throw new UnsupportedOperationException();
    }

    public static void setup(Header baseHeader, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        initEventHandler((timestamp, headerOverride, event) -> {
            Header header = baseHeader.cloneAndOverride(headerOverride);
            eventProcessor.offer(timestamp, header, event);
        });
        // Get Events of NEW applications
        new RMAppImplTracer().installOn(instrumentation);
        // Get Apps in the RM every 10s
        new RMContextImplThread().installOn(instrumentation);
    }

    public static TriConsumer<Long, Header, Object> getEventHandler() {
        return eventHandler;
    }

    public static void initEventHandler(TriConsumer<Long, Header, Object> eventHandler) {
        RMAppTracer.eventHandler = eventHandler;
    }

    public static class RMContextImplThread extends ConstructorTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return named("org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> constructorMatcher() {
            return any();
        }

        @Override
        protected Implementation newImplementation() {
            return Advice.to(RMContextImplThread.class);
        }

        @Advice.OnMethodExit
        public static void runEventScheduler(@Advice.This Object o) {
            new ScheduledThreadPoolExecutor(1, new GarmadonWorkerThreadFactory())
                    .scheduleAtFixedRate(
                            new RMContextImplEventRunnable((RMContextImpl) o, getEventHandler()),
                            0,
                            10,
                            TimeUnit.SECONDS);
        }

        public static class GarmadonWorkerThreadFactory implements ThreadFactory {

            public Thread newThread(Runnable r) {
                Thread garmadonThread = new Thread(r);
                garmadonThread.setName("GARMADON - App listing thread");
                garmadonThread.setDaemon(true);
                return garmadonThread;
            }
        }
    }

    public static class RMAppImplTracer extends ConstructorTracer {

        @Override
        protected ElementMatcher<? super TypeDescription> typeMatcher() {
            return named("org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl");
        }

        @Override
        protected ElementMatcher<? super MethodDescription> constructorMatcher() {
            return any();
        }

        @Override
        protected Implementation newImplementation() {
            return to(RMAppImplTracer.class).andThen(SuperMethodCall.INSTANCE);
        }

        public static void intercept(@Argument(0) ApplicationId applicationId, @Argument(3) String name,
                                     @Argument(4) String user, @Argument(5) String queue,
                                     @Argument(9) long submitTime, @Argument(10) String applicationType) throws Exception {
            try {
                Header header = Header.newBuilder()
                        .withApplicationID(applicationId.toString())
                        .withUser(user)
                        .withApplicationName(name)
                        .withFramework(applicationType.toUpperCase())
                        .build();

                ResourceManagerEventProtos.ApplicationEvent event = ResourceManagerEventProtos.ApplicationEvent.newBuilder()
                        .setState(State.NEW.name())
                        .setQueue(queue)
                        .build();

                eventHandler.accept(submitTime, header, event);
            } catch (Exception ignored) {
            }
        }
    }
}
