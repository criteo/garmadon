package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

public class GarmadonMessageFilters {

    public static GarmadonMessageFilter acceptAll(){
        return new AcceptAllFilter();
    }

    public static class AcceptAllFilter implements GarmadonMessageFilter {

        @Override
        public boolean acceptsType(int type) {
            return true;
        }

        @Override
        public boolean acceptsHeader(DataAccessEventProtos.Header header) {
            return true;
        }

    }

}
