package com.criteo.hadoop.garmadon.hdfs.writer;

import akka.Done;
import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.FI;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static akka.pattern.Patterns.ask;

/**
 * A writer that manages a pool of PartitionedWriters that are scheduled on a pool of threads
 * <p>
 * Each PartitionedWriter has the illusion of working on a single thread, while we still avoid using 1 thread per PartitionedWriter
 * <p>
 * Writers must be subscribed before this writer is started.
 * The latest writer subscribed for an event type is the one chosen to dispatch messages.
 * <p>
 * AsyncWriter asyncWriter = new AsyncWriter();
 * asyncWriter.subscribe("event_1", writer1);
 * asyncWriter.subscribe("event_1", writer1);
 * .
 * .
 * .
 * asyncWriter.start(uncaughtExceptionHandler)
 * <p>
 * Adding new writers after start will not be taken into account
 * <p>
 * AsyncWriter needs to be started before any interaction with it
 * <p>
 * About asynchronous methods return type.
 * <p>
 * Asynchronous returns either a CompletableFuture, either void.
 * <p>
 * In both cases, when an exception occur during an asynchronous process, it will trigger the uncaught exception handler.
 * <p>
 * When a CompletableFuture is returned, the exception will also be returned, as a failed CompletableFuture
 *
 * @param <M> The type of messages to be processed
 */
public class AsyncWriter<M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWriter.class);

    private static final Duration MAX_AKKA_DELAY = Duration.ofSeconds(21474835);

    private final Map<String, PartitionedWriter<M>> writers = new HashMap<>();
    private final Map<String, ActorRef> actors = new HashMap<>();

    private ActorRef writersManager;

    public AsyncWriter() {
    }

    public void subscribe(String eventName, PartitionedWriter<M> writer) {
        writers.put(eventName, writer);
    }

    public void start(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        start(ActorSystem.create(), uncaughtExceptionHandler);
    }

    void start(ActorSystem actorSystem, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        writersManager = actorSystem.actorOf(ManagerActor.props(uncaughtExceptionHandler), "async-writer-manager-actor");
        writers.forEach((event, writer) -> {
            try {
                ActorRef ref = (ActorRef) ask(writersManager, new ManagerActor.Subscribe(event, writer), Duration.ofSeconds(30)).toCompletableFuture().get();
                actors.put(event, ref);
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("could not start actor writer for event " + event, e);
            }
        });
    }

    /**
     * Closes all underlying writers
     * <p>
     * The writer is still usable but will operate on new files
     * <p>
     * Exception during close will call uncaught exception handler and be returned in the completable future
     *
     * @return CompletableFuture completed when all underlying writers are closed (or failed doing it). Can complete exceptionally.
     */
    public CompletableFuture<Void> close() {
        return askAll(new WriterActor.CloseEvent());
    }

    /**
     * Dispatch messages to the correct writer for an event name previously registered
     * <p>
     * Exceptions during writing process will be caught be the uncaught exception handler provided at start
     *
     * @param eventName Used to redirect the message to the previously registered writer for the event name
     * @param when      Timestamp of the message
     * @param offset    The offset of the message sent
     * @param msg       Message provided via a supplier.
     *                  This helps any computation associated to this message production (proto conversion for instance)
     *                  to be executed in the asynchronous domain.
     */
    public void dispatch(String eventName, Instant when, Offset offset, Supplier<M> msg) {
        ActorRef ref = actors.get(eventName);
        if (ref == null) {
            LOGGER.warn("no subscribed writer for " + eventName + " to handle msg. Skipping it.");
        } else {
            ref.tell(new WriterActor.WriteEvent(when, offset, msg), null);
        }
    }

    /**
     * Drop partition for all underlying writers
     * <p>
     * Exception during close will call uncaught exception handler and be returned in the completable future
     *
     * @param partition The partition to drop
     * @return CompletableFuture completed when all underlying writers dropped the partition (or failed doing it). Can complete exceptionally.
     */
    public CompletableFuture<Void> dropPartition(int partition) {
        return askAll(new WriterActor.DropPartitionEvent(partition));
    }

    /**
     * Expire consumers for underlying writers.
     * <p>
     * Exceptions during writing process will be caught be the uncaught exception handler provided at start
     */
    public void expireConsumers() {
        tellAll(new WriterActor.ExpireConsumersEvent());
    }

    /**
     * Get starting offsets from all underlying writers and aggregate them to define the lowest per partition
     * <p>
     * Exception during close will call uncaught exception handler and be returned in the completable future
     *
     * @param partitions Partitions for which to collect starting offsets
     * @return CompletableFuture completed when all underlying writers have returned their failing offsets.
     * If one fails, the resulting CompletableFuture will complete exceptionally
     */
    public CompletableFuture<Map<Integer, Long>> getStartingOffsets(Collection<Integer> partitions) {
        return actors
            .values()
            .stream()
            .map(actor -> getStartingOffsets(actor, partitions))
            .reduce(
                CompletableFuture.completedFuture(new HashMap<>()),
                (futureChain, startingOffsetsFuture) -> futureChain.thenCombine(startingOffsetsFuture, this::mergeStartingOffsets)
            );
    }

    private CompletableFuture<Map<Integer, Long>> getStartingOffsets(ActorRef writer, Collection<Integer> partitions) {
        return ask(writer, new WriterActor.GetStartingOffsetsEvent(partitions), MAX_AKKA_DELAY).toCompletableFuture().thenApply(o -> (Map<Integer, Long>) o);
    }

    private Map<Integer, Long> mergeStartingOffsets(Map<Integer, Long> offsets1, Map<Integer, Long> offsets2) {
        offsets2.forEach((partition, offset) -> offsets1.merge(partition, offset, this::mergeStartingOffsetsForPartition));
        return offsets1;
    }

    private Long mergeStartingOffsetsForPartition(Long o1, Long o2) {
        if (o1 == OffsetComputer.NO_OFFSET) {
            return o2;
        }
        if (o2 == OffsetComputer.NO_OFFSET) {
            return o1;
        }
        return Long.min(o1, o2);
    }

    /**
     * Heartbeats underlying writers
     * <p>
     * Exceptions during heartbeat process will be caught be the uncaught exception handler provided at start
     *
     * @param partition The partition for which to heartbeat
     * @param offset    The current offset at the moment of heartbeat
     */
    public void heartbeat(int partition, Offset offset) {
        tellAll(new WriterActor.HeartbeatEvent(partition, offset));
    }

    private void tellAll(Object event) {
        actors
            .values()
            .forEach(actor -> actor.tell(event, null));
    }

    private CompletableFuture<Void> askAll(Object event) {
        CompletableFuture[] futures = actors
            .values()
            .stream()
            .map(actor -> ask(actor, event, MAX_AKKA_DELAY).toCompletableFuture())
            .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    /**
     * Parent of all writer actors
     * <p>
     * Calls the uncaught exception handler when a writer actor throws an unexpected exception
     * and prevent restart of the child actor (Akka default)
     */
    static class ManagerActor extends AbstractActor {

        private final SupervisorStrategy strategy;

        ManagerActor(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
            //The manager will not make writers failed (hence resume) and will call the uncaught exception handler
            this.strategy = new OneForOneStrategy(
                DeciderBuilder.matchAny(ex -> {
                    LOGGER.error("Caught unmanaged exception", ex);
                    uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), ex);
                    return SupervisorStrategy.resume();
                }).build()
            );
        }

        private static Props props(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
            return Props.create(ManagerActor.class, () -> new ManagerActor(uncaughtExceptionHandler));
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void postRestart(Throwable reason) {
            LOGGER.info("Started AsyncWriter manager actor");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(Subscribe.class, subscribe -> {
                    ActorRef ref = getContext().actorOf(WriterActor.props(subscribe.partitionedWriter), "partitioned-writer-actor-" + subscribe.event);
                    LOGGER.info("AsyncWriter manager actor subscribed new writer for event " + subscribe.event);

                    getSender().tell(ref, getSelf());
                })
                .build();
        }

        static class Subscribe {

            private final String event;
            private final PartitionedWriter partitionedWriter;

            Subscribe(String event, PartitionedWriter partitionedWriter) {
                this.event = event;
                this.partitionedWriter = partitionedWriter;
            }

        }
    }

    /**
     * Wraps a partitioned writer to provide multithreading implemented (almost) as-in single thread
     */
    static class WriterActor extends AbstractActor {

        private final PartitionedWriter<Object> writer;

        WriterActor(PartitionedWriter<Object> writer) {
            this.writer = writer;
        }

        private static <M> Props props(PartitionedWriter<M> writer) {
            return Props.create(WriterActor.class, () -> new WriterActor((PartitionedWriter<Object>) writer));
        }

        @Override
        public AbstractActor.Receive createReceive() {
            return receiveBuilder()
                .match(CloseEvent.class, replyExceptionsAndRethrow(evt -> {
                    doClose();
                    done();
                }))
                .match(DropPartitionEvent.class, replyExceptionsAndRethrow(evt -> {
                    doDropPartition(evt.partition);
                    done();
                }))
                .match(ExpireConsumersEvent.class, evt -> doExpireConsumers())
                .match(GetStartingOffsetsEvent.class, replyExceptionsAndRethrow(evt -> {
                    try {
                        reply(doGetStartingOffsets(evt.partitions));
                    } catch (IOException e) {
                        replyFailure(e);
                    }
                }))
                .match(HeartbeatEvent.class, evt -> doHeartbeat(evt.partition, evt.offset))
                .match(WriteEvent.class, evt -> doWrite(evt.when, evt.offset, evt.msgSupplier.get()))
                .build();
        }

        private void doClose() {
            writer.close();
        }

        private void doDropPartition(int p) {
            writer.dropPartition(p);
        }

        private void doExpireConsumers() {
            writer.expireConsumers();
        }

        private Map<Integer, Long> doGetStartingOffsets(Collection<Integer> partitions) throws IOException {
            return writer.getStartingOffsets(partitions);
        }

        private void doHeartbeat(int p, Offset o) {
            writer.heartbeat(p, o);
        }

        private void doWrite(Instant when, Offset offset, Object msg) {
            writer.write(when, offset, msg);
        }

        private void done() {
            getSender().tell(Done.getInstance(), getSelf());
        }

        private void reply(Object o) {
            getSender().tell(o, getSelf());
        }

        private <P> FI.UnitApply<P> replyExceptionsAndRethrow(FI.UnitApply<P> action) {
            return p -> {
                try {
                    action.apply(p);
                } catch (Exception e) {
                    replyFailure(e);
                    throw e;
                }
            };
        }

        private void replyFailure(Exception e) {
            getSender().tell(new Status.Failure(e), getSelf());
        }

        /* Events the actor can receive */
        static class WriteEvent {

            private final Instant when;
            private final Supplier msgSupplier;
            private final Offset offset;

            WriteEvent(Instant when, Offset offset, Supplier msgSupplier) {
                this.when = when;
                this.offset = offset;
                this.msgSupplier = msgSupplier;
            }
        }

        static class CloseEvent {
        }

        static class DropPartitionEvent {

            private final int partition;

            DropPartitionEvent(int partition) {
                this.partition = partition;
            }
        }

        static class ExpireConsumersEvent {

        }

        static class GetStartingOffsetsEvent {

            private final Collection<Integer> partitions;

            GetStartingOffsetsEvent(Collection<Integer> partitions) {
                this.partitions = partitions;
            }
        }

        static class HeartbeatEvent {

            private final int partition;
            private final Offset offset;

            HeartbeatEvent(int partition, Offset offset) {
                this.partition = partition;
                this.offset = offset;
            }
        }

    }

}
