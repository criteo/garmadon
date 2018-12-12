package com.criteo.hadoop.garmadon.elasticsearch;

import java.util.Map;

public class Configuration {
    private ElasticSearch elasticsearch;

    public ElasticSearch getElasticsearch() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticSearch elasticsearch) {
        this.elasticsearch = elasticsearch;
    }

    public class ElasticSearch {
        private Map<String, String> settings;

        public Map<String, String> getSettings() {
            return settings;
        }

        public void setSettings(Map<String, String> settings) {
            this.settings = settings;
        }
    }
}
