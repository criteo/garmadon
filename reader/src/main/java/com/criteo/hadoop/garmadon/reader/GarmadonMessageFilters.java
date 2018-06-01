package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.schema.events.Header;

public class GarmadonMessageFilters {

    public static GarmadonMessageFilter.HeaderFilter hasTag(Header.Tag tag){
        return new GarmadonMessageFilter.HeaderFilter.TagFilter(tag);
    }

    public static GarmadonMessageFilter.HeaderFilter hasContainerId(String id){
        return new GarmadonMessageFilter.HeaderFilter.ContainerFilter(id);
    }

    public static GarmadonMessageFilter.TypeFilter hasType(int type) {
        return new GarmadonMessageFilter.TypeFilter(type);
    }

    public static GarmadonMessageFilter.NotFilter not(GarmadonMessageFilter other) {
        return new GarmadonMessageFilter.NotFilter(other);
    }
}
