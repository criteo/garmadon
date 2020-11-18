package com.criteo.hadoop.garmadon.forwarder.channel;

import com.criteo.hadoop.garmadon.forwarder.handler.*;
import com.criteo.hadoop.garmadon.forwarder.kafka.KafkaService;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.util.Set;

public class ForwarderChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final KafkaService kafkaService;
    private final Set<Integer> broadcastedTypes;

    public ForwarderChannelInitializer(KafkaService kafkaService, Set<Integer> broadcastedTypes) {
        this.kafkaService = kafkaService;
        this.broadcastedTypes = broadcastedTypes;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        socketChannel.pipeline()
                .addLast(GreetingDecoder.class.getSimpleName(), new GreetingDecoder())
                .addLast(GreetingHandler.class.getSimpleName(), new GreetingHandler())
                .addLast(EventDecoder.class.getSimpleName(), new EventDecoder())
                .addLast(EventHandler.class.getSimpleName(), new EventHandler(broadcastedTypes))
                .addLast(KafkaHandler.class.getSimpleName(), new KafkaHandler(kafkaService));
    }
}
