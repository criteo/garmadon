package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;

public class GreetingDecoderTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    @Before
    public void executedBeforeEach() {
        GreetingDecoder greetingDecode = new GreetingDecoder();
        channel.get().pipeline().addLast(GreetingDecoder.class.getSimpleName(), greetingDecode);
    }

    @Test
    public void GreetingDecoder_should_read_4_bytes_at_once() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        Assert.assertTrue(channel.get().writeInbound(byteBuf));
        Assert.assertTrue(channel.get().finish());

        Assert.assertEquals(Unpooled.wrappedBuffer(bytes), channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void GreetingDecoder_should_read_greetings_in_chunks() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        Assert.assertFalse(channel.get().writeInbound(byteBuf.readBytes(2)));
        Assert.assertTrue(channel.get().writeInbound(byteBuf.readBytes(2)));
        Assert.assertTrue(channel.get().finish());

        Assert.assertEquals(Unpooled.wrappedBuffer(bytes), channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void GreetingDecoder_should_remove_itself_from_the_pipeline_once_returned_greetings(){
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        Assert.assertTrue(channel.get().writeInbound(byteBuf));

        assertNull(channel.get().pipeline().toMap().get(GreetingDecoder.class.getSimpleName()));
    }
}
