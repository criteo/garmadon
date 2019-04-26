package com.criteo.hadoop.garmadon.elasticsearch.cache;

import java.util.List;
import java.util.Objects;

public class AppEventEnrichment {
    private String applicationName;
    private String framework;
    private String amContainerId;
    private String username;
    private List<String> yarnTags;

    public AppEventEnrichment(String applicationName, String framework, String amContainerId, String username, List<String> yarnTags) {
        this.applicationName = applicationName;
        this.framework = framework;
        this.amContainerId = amContainerId;
        this.username = username;
        this.yarnTags = yarnTags;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getFramework() {
        return framework;
    }

    public String getAmContainerId() {
        return amContainerId;
    }

    public String getUsername() {
        return username;
    }

    public List<String> getYarnTags() {
        return yarnTags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppEventEnrichment that = (AppEventEnrichment) o;
        return Objects.equals(applicationName, that.applicationName) &&
            Objects.equals(framework, that.framework) &&
            Objects.equals(amContainerId, that.amContainerId) &&
            Objects.equals(username, that.username) &&
            Objects.equals(yarnTags, that.yarnTags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationName, framework, amContainerId, username, yarnTags);
    }

    @Override
    public String toString() {
        return "AppEventEnrichment{" +
            "applicationName='" + applicationName + '\'' +
            ", framework='" + framework + '\'' +
            ", amContainerId='" + amContainerId + '\'' +
            ", username='" + username + '\'' +
            ", yarnTags=" + yarnTags +
            '}';
    }
}
