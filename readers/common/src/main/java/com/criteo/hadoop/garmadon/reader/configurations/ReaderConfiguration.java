package com.criteo.hadoop.garmadon.reader.configurations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;

public class ReaderConfiguration {

    protected ReaderConfiguration() {
        throw new UnsupportedOperationException();
    }

    public static <T> T loadConfig(Class<T> clazz) throws IOException {
        return loadConfig(clazz, "garmadon-config.yml");
    }

    public static <T> T loadConfig(Class<T> clazz, String confFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(ReaderConfiguration.class.getClassLoader()
                .getResourceAsStream(confFile), clazz);
    }
}