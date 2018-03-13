package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

public interface GarmadonMessageFilter {

    boolean acceptsType(int type);

    boolean acceptsHeader(DataAccessEventProtos.Header header);

}
