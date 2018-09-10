package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

import java.util.ArrayList;
import java.util.List;

// TODO: remove tag once tags is used everywhere
public class Header {

    protected final String applicationID;
    protected final String appAttemptID;
    protected final String applicationName;
    protected final String user;
    protected final String containerID;
    protected final String hostname;
    protected final String tag;
    protected final String pid;
    protected final String framework;
    protected final String component;
    protected final String executorId;
    protected final List<String> tags;

    public enum Tag {
        YARN_APPLICATION,
        FORWARDER,
        NODEMANAGER
    }

    public Header(String applicationID, String appAttemptID, String applicationName,
                  String user, String containerID, String hostname, String tag, List<String> tags, String pid,
                  String framework, String component, String executorId) {
        this.applicationID = applicationID;
        this.appAttemptID = appAttemptID;
        this.applicationName = applicationName;
        this.user = user;
        this.containerID = containerID;
        this.hostname = hostname;
        this.tag = tag;
        this.tags = tags;
        this.pid = pid;
        this.framework = framework;
        this.component = component;
        this.executorId = executorId;
    }

    public byte[] serialize() {
        DataAccessEventProtos.Header.Builder builder = DataAccessEventProtos.Header
                .newBuilder();
        if (applicationID != null)
            builder.setApplicationId(applicationID);
        if (appAttemptID != null)
            builder.setAppAttemptID(appAttemptID);
        if (applicationName != null)
            builder.setApplicationName(applicationName);
        if (user != null)
            builder.setUserName(user);
        if (containerID != null)
            builder.setContainerId(containerID);
        if (hostname != null)
            builder.setHostname(hostname);
        if (tag != null)
            builder.setTag(tag);
        if (tags != null && tags.size() > 0)
            for (String tag : tags) {
                builder.addTags(tag);
            }
        if (pid != null)
            builder.setPid(pid);
        if (framework != null)
            builder.setFramework(framework);
        if (component != null)
            builder.setComponent(component);
        if (executorId != null)
            builder.setExecutorId(executorId);
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return "Header{" +
                "applicationID='" + applicationID + '\'' +
                ", appAttemptID='" + appAttemptID + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", user='" + user + '\'' +
                ", containerID='" + containerID + '\'' +
                ", hostname='" + hostname + '\'' +
                ", tag='" + tag + '\'' +
                ", pid='" + pid + '\'' +
                ", framework='" + framework + '\'' +
                ", component='" + component + '\'' +
                ", executorId='" + executorId + '\'' +
                ", tags=" + tags +
                '}';
    }

    /**
     * Header that can be cloned with overridden values
     */
    public static class BaseHeader extends Header {
        private BaseHeader(String applicationID, String appAttemptID, String applicationName, String user,
                           String containerID, String hostname, String tag, List<String> tags, String pid, String framework,
                           String component, String executorId) {
            super(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag, tags, pid, framework,
                    component, executorId);
        }

        public Header cloneAndOverride(Header override) {
            String applicationID = override.applicationID != null ? override.applicationID : this.applicationID;
            String appAttemptID = override.appAttemptID != null ? override.appAttemptID : this.appAttemptID;
            String applicationName = override.applicationName != null ? override.applicationName : this.applicationName;
            String user = override.user != null ? override.user : this.user;
            String containerID = override.containerID != null ? override.containerID : this.containerID;
            String hostname = override.hostname != null ? override.hostname : this.hostname;
            String tag = override.tag != null ? override.tag : this.tag;
            List<String> tags = (override.tags != null && override.tags.size() > 0) ? override.tags : this.tags;
            String pid = override.pid != null ? override.pid : this.pid;
            String framework = override.framework != null ? override.framework : this.framework;
            String component = override.component != null ? override.component : this.component;
            String executorId = override.executorId != null ? override.executorId : this.executorId;
            return new Header(
                    applicationID,
                    appAttemptID,
                    applicationName,
                    user,
                    containerID,
                    hostname,
                    tag,
                    tags,
                    pid,
                    framework,
                    component,
                    executorId
            );
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String applicationID;
        private String appAttemptID;
        private String applicationName;
        private String user;
        private String containerID;
        private String hostname;
        private String tag;
        private String pid;
        private String framework;
        private String component;
        private String executorId;
        private List<String> tags = new ArrayList<>();

        Builder() {
        }

        public Builder withHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder withApplicationID(String applicationID) {
            this.applicationID = applicationID;
            return this;
        }

        public Builder withApplicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder withAppAttemptID(String appAttemptID) {
            this.appAttemptID = appAttemptID;
            return this;
        }


        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withContainerID(String containerID) {
            this.containerID = containerID;
            return this;
        }

        public Builder withTag(String tag) {
            this.tag = tag;
            return this;
        }

        public Builder addTag(String tag) {
            this.tags.add(tag);
            return this;
        }

        public Builder withPid(String pid) {
            this.pid = pid;
            return this;
        }

        public Builder withFramework(String framework) {
            this.framework = framework;
            return this;
        }

        public Builder withComponent(String component) {
            this.component = component;
            return this;
        }


        public Builder withExecutorId(String executorId) {
            this.executorId = executorId;
            return this;
        }

        public Header build() {
            return new Header(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag, tags,
                    pid, framework, component, executorId);
        }

        public BaseHeader buildBaseHeader() {
            return new BaseHeader(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag,
                    tags, pid, framework, component, executorId);
        }
    }
}
