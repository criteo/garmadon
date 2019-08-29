package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.DelegatingWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class ExtraMetadataWriteSupport<T> extends DelegatingWriteSupport<T> implements BiConsumer<String, String> {

    private Map<String, String> extraMetaData = new HashMap<>();

    public ExtraMetadataWriteSupport(WriteSupport<T> delegate) {
        super(delegate);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        WriteContext parent = super.init(configuration);
        extraMetaData.putAll(parent.getExtraMetaData());
        return new WriteContext(parent.getSchema(), extraMetaData);
    }

    public void accept(String key, String value) {
        extraMetaData.put(key, value);
    }
}
