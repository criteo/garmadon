package com.criteo.hadoop.garmadon.forwarder.message;

public class BroadCastedKafkaMessage extends KafkaMessage {

    public BroadCastedKafkaMessage(byte[] value) {
        super(value);
    }

    @Override
    public boolean isBroadCasted() {
        return true;
    }

}
