package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class GreetingHandlerTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();
    private ChannelHandler mockedKafkaHandler;

    @Before
    public void executedBeforeEach() {
        GreetingHandler greetingHandler = new GreetingHandler();
        mockedKafkaHandler = mock(ChannelHandler.class);
        channel.get().pipeline().addLast(GreetingHandler.class.getSimpleName(), greetingHandler);
        channel.get().pipeline().addLast(KafkaHandler.class.getSimpleName(), mockedKafkaHandler);
    }

    @Test
    public void GreetingHandler_should_send_back_a_greeting_when_inout_greeting_is_correct() throws ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(ProtocolVersion.GREETINGS);
        channel.get().writeInbound(byteBuf);

        ByteBuf output = channel.get().readOutbound();
        byte[] greetings = new byte[4];
        output.readBytes(greetings); //when reading output we must copy bytes because it is a direct bytebuf
        ProtocolVersion.checkVersion(greetings);
    }

    @Test
    public void GreetingHandler_should_close_cnx_on_bad_greetings() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        channel.get().writeInbound(byteBuf);

        assertFalse(channel.get().isOpen());
    }

    @Test
    public void GreetingHandler_should_remove_itself_on_good_greetings(){
        ByteBuf byteBuf = Unpooled.wrappedBuffer(ProtocolVersion.GREETINGS);
        channel.get().writeInbound(byteBuf);

        assertNull(channel.get().pipeline().toMap().get(GreetingDecoder.class.getSimpleName()));
    }

    @Test
    public void GreetingHandler_should_install_CloseHandler_before_KafkaHandler_after_receiving_good_greetings(){
        ByteBuf byteBuf = Unpooled.wrappedBuffer(ProtocolVersion.GREETINGS);
        channel.get().writeInbound(byteBuf);

        Iterator<Map.Entry<String, ChannelHandler>> handlerIterator = channel.get().pipeline().iterator();
        assertThat(handlerIterator.next().getValue().getClass(), equalTo(CloseHandler.class));
        //mocked kafka handler is placed after
        assertThat(handlerIterator.next().getValue(), equalTo(mockedKafkaHandler));
    }

}
