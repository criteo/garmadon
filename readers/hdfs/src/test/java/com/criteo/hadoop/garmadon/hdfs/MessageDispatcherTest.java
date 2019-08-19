package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.MessageDispatcher.OneByOneMultiQueue.DispatchableMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class MessageDispatcherTest {

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(1); //using one thread helps reasoning about ordering of events
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void OneByOneMultiQueueShouldBlockUntilOneSubQueueWithOneMessageIsAvailable_startingFromNothing() throws InterruptedException, ExecutionException, TimeoutException {
        MessageDispatcher.OneByOneMultiQueue q = new MessageDispatcher.OneByOneMultiQueue();

        Future<DispatchableMessage> futureMsg = executor.submit(q::getNextMessage);

        Thread.sleep(500);

        assertFalse(futureMsg.isDone());

        Object o = new Object();
        q.put(0, 0, o);

        Thread.sleep(500);

        assertTrue(futureMsg.isDone());

        DispatchableMessage dm = futureMsg.get();
        checkDispatchableMessageIs(dm, 0, 0, o);
    }

    @Test
    public void OneByOneMultiQueueShouldBlockUntilOneSubQueueWithOneMessageIsAvailable_afterStarvation() throws InterruptedException, ExecutionException {
        MessageDispatcher.OneByOneMultiQueue q = new MessageDispatcher.OneByOneMultiQueue();
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        q.put(0, 0, o1);
        q.put(1, 0, o2);
        q.put(3, 0, o3);

        q.getNextMessage().getMsg().release();
        q.getNextMessage().getMsg().release();
        q.getNextMessage().getMsg().release();

        Future<DispatchableMessage> futureMsg = executor.submit(q::getNextMessage);

        Thread.sleep(500);

        assertFalse(futureMsg.isDone());

        Object o = new Object();
        q.put(0, 0, o);

        Thread.sleep(500);

        assertTrue(futureMsg.isDone());

        DispatchableMessage dm = futureMsg.get();
        checkDispatchableMessageIs(dm, 0, 0, o);

    }

    @Test
    public void OneByOneMultiQueueShouldNotGiveOtherMessagesFromSubqueueUntilCurrentInUseIsReleased() throws InterruptedException, ExecutionException, TimeoutException {
        MessageDispatcher.OneByOneMultiQueue q = new MessageDispatcher.OneByOneMultiQueue();
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        q.put(0, 0, o1);
        q.put(0, 0, o2);
        q.put(0, 1, o3);
        q.put(0, 1, o4);

        Future<DispatchableMessage> futureMsg1 = executor.submit(q::getNextMessage);
        Future<DispatchableMessage> futureMsg2 = executor.submit(q::getNextMessage);
        Future<DispatchableMessage> futureMsg3 = executor.submit(q::getNextMessage);
        Future<DispatchableMessage> futureMsg4 = executor.submit(q::getNextMessage);
        Future<DispatchableMessage> futureMsg5 = executor.submit(q::getNextMessage);

        //because of round robin, we are sure the futureMsg1 contains o1 and futureMsg2 contains o3

        //we should be able to get two messages right away
        DispatchableMessage msg1 = futureMsg1.get(1, TimeUnit.SECONDS);
        DispatchableMessage msg2 = futureMsg2.get(1, TimeUnit.SECONDS);

        Thread.sleep(1000);

        assertFalse(futureMsg3.isDone());
        assertFalse(futureMsg4.isDone());
        assertFalse(futureMsg5.isDone());

        //if we add a new message on another eventType, it should be our next futureMsg
        Object o5 = new Object();
        q.put(1, 0, o5);

        Thread.sleep(500);

        assertTrue(futureMsg3.isDone());
        checkDispatchableMessageIs(futureMsg3.get(), 1, 0, o5);
        assertFalse(futureMsg4.isDone());
        assertFalse(futureMsg5.isDone());

        //if we release msg1, o2 should be our next message
        msg1.getMsg().release();

        Thread.sleep(500);

        assertTrue(futureMsg4.isDone());
        checkDispatchableMessageIs(futureMsg4.get(), 0, 0, o2);
        assertFalse(futureMsg5.isDone());

        //if we release msg2, o4 should be our next message
        msg2.getMsg().release();

        Thread.sleep(500);

        assertTrue(futureMsg5.isDone());
        checkDispatchableMessageIs(futureMsg5.get(), 0, 1, o4);
    }

    private void checkDispatchableMessageIs(DispatchableMessage dm, int eventType, int partition, Object o) {
        assertThat("eventType is not the one expected", dm.getEventType(), is(eventType));
        assertThat("partition is not the one expected", dm.getPartition(), is(partition));
        assertThat("object grabbed from the queue is not the one expected", dm.getMsg().getValue(), is(o));
    }

}
