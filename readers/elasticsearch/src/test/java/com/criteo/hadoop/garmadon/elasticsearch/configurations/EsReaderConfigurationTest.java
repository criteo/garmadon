package com.criteo.hadoop.garmadon.elasticsearch.configurations;


import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.Test;

import java.io.IOException;

import static com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration.loadConfig;
import static org.junit.Assert.assertEquals;

public class EsReaderConfigurationTest {

    @Test
    public void getConfig() throws IOException {
        EsReaderConfiguration config = loadConfig(EsReaderConfiguration.class, "garmadon-config.yml");
        assertEquals("elasticsearch", config.getElasticsearch().getHost());
        assertEquals(500, config.getElasticsearch().getBulkActions());
        assertEquals(10, config.getElasticsearch().getBulkSizeMB());
    }

    @Test(expected = IOException.class)
    public void failedGetConfigAsFileNotAvailable() throws IOException {
        ReaderConfiguration.loadConfig(EsReaderConfiguration.class, "garmadon-not-existing-config.yml");
    }

    @Test(expected = UnrecognizedPropertyException.class)
    public void failedGetUnknownProperties() throws IOException {
        ReaderConfiguration.loadConfig(EsReaderConfiguration.class, "garmadon-bad-config.yml");
    }
}
