package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Header {

    protected final String id;
    protected final String applicationID;
    protected final String appAttemptID;
    protected final String applicationName;
    protected final String user;
    protected final String containerID;
    protected final String hostname;
    protected final String pid;
    protected final String framework;
    protected final String component;
    protected final String executorId;
    protected final String mainClass;
    protected final List<String> tags;

    public enum Tag {
        YARN_APPLICATION,
        FORWARDER,
        RESOURCEMANAGER,
        NODEMANAGER,
        STANDALONE
    }

    public Header(String id, String applicationID, String appAttemptID, String applicationName,
                  String user, String containerID, String hostname, List<String> tags, String pid,
                  String framework, String component, String executorId, String mainClass) {
        this.id = id;
        this.applicationID = applicationID;
        this.appAttemptID = appAttemptID;
        this.applicationName = applicationName;
        this.user = user;
        this.containerID = containerID;
        this.hostname = hostname;
        this.tags = tags;
        this.pid = pid;
        this.framework = framework;
        this.component = component;
        this.executorId = executorId;
        this.mainClass = mainClass;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Header))
            return false;

        Header otherHeader = (Header) other;

        return Objects.equals(this.id, otherHeader.id) && Objects.equals(otherHeader.appAttemptID, this.appAttemptID) &&
                Objects.equals(this.applicationID, otherHeader.applicationID) &&
                Objects.equals(this.appAttemptID, otherHeader.appAttemptID) &&
                Objects.equals(this.applicationName, otherHeader.applicationName) &&
                Objects.equals(this.user, otherHeader.user) &&
                Objects.equals(this.containerID, otherHeader.containerID) &&
                Objects.equals(this.hostname, otherHeader.hostname) &&
                Objects.equals(this.tags, otherHeader.tags) && Objects.equals(this.pid, otherHeader.pid) &&
                Objects.equals(this.framework, otherHeader.framework) &&
                Objects.equals(this.component, otherHeader.component) &&
                Objects.equals(this.executorId, otherHeader.executorId) &&
                Objects.equals(this.mainClass, otherHeader.mainClass);
    }

    public Header cloneAndOverride(Header override) {
        String id = override.id != null ? override.id : this.id;
        String applicationID = override.applicationID != null ? override.applicationID : this.applicationID;
        String appAttemptID = override.appAttemptID != null ? override.appAttemptID : this.appAttemptID;
        String applicationName = override.applicationName != null ? override.applicationName : this.applicationName;
        String user = override.user != null ? override.user : this.user;
        String containerID = override.containerID != null ? override.containerID : this.containerID;
        String hostname = override.hostname != null ? override.hostname : this.hostname;
        List<String> tags = (override.tags != null && override.tags.size() > 0) ? override.tags : this.tags;
        String pid = override.pid != null ? override.pid : this.pid;
        String framework = override.framework != null ? override.framework : this.framework;
        String component = override.component != null ? override.component : this.component;
        String executorId = override.executorId != null ? override.executorId : this.executorId;
        String mainClass = override.mainClass != null ? override.mainClass : this.mainClass;
        return new Header(
                id,
                applicationID,
                appAttemptID,
                applicationName,
                user,
                containerID,
                hostname,
                tags,
                pid,
                framework,
                component,
                executorId,
                mainClass
        );
    }

    public SerializedHeader toSerializeHeader() {
        return new SerializedHeader(id, applicationID, appAttemptID, applicationName, user, containerID, hostname, tags, pid, framework,
                component, executorId, mainClass);
    }

    public byte[] serialize() {
        EventHeaderProtos.Header.Builder builder = EventHeaderProtos.Header
                .newBuilder();
        if (id != null)
            builder.setId(id);
        if (applicationID != null)
            builder.setApplicationId(applicationID);
        if (appAttemptID != null)
            builder.setAppAttemptId(appAttemptID);
        if (applicationName != null)
            builder.setApplicationName(applicationName);
        if (user != null)
            builder.setUserName(user);
        if (containerID != null)
            builder.setContainerId(containerID);
        if (hostname != null)
            builder.setHostname(hostname);
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
        if (mainClass != null)
            builder.setMainClass(mainClass);
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return "Header{" +
                "id='" + id + '\'' +
                ", applicationID='" + applicationID + '\'' +
                ", appAttemptID='" + appAttemptID + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", user='" + user + '\'' +
                ", containerID='" + containerID + '\'' +
                ", hostname='" + hostname + '\'' +
                ", pid='" + pid + '\'' +
                ", framework='" + framework + '\'' +
                ", component='" + component + '\'' +
                ", executorId='" + executorId + '\'' +
                ", mainClass='" + mainClass + '\'' +
                ", tags=" + tags +
                '}';
    }

    public static class SerializedHeader extends Header {
        private final byte[] bytes;

        SerializedHeader(String id, String applicationID, String appAttemptID, String applicationName, String user,
                         String containerID, String hostname, List<String> tags, String pid, String framework,
                         String component, String executorId, String mainClass) {
            super(id, applicationID, appAttemptID, applicationName, user, containerID, hostname, tags, pid, framework,
                    component, executorId, mainClass);
            this.bytes = super.serialize();
        }

        @Override
        public byte[] serialize() {
            return bytes;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private String applicationID;
        private String appAttemptID;
        private String applicationName;
        private String user;
        private String containerID;
        private String hostname;
        private String pid;
        private String framework;
        private String component;
        private String executorId;
        private String mainClass;
        private List<String> tags = new ArrayList<>();

        Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
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

        public Builder withMainClass(String mainClass) {
            this.mainClass = mainClass;
            return this;
        }

        public Header build() {
            return new Header(id, applicationID, appAttemptID, applicationName, user, containerID, hostname, tags,
                    pid, framework, component, executorId, mainClass);
        }

        public SerializedHeader buildSerializedHeader() {
            return new SerializedHeader(id, applicationID, appAttemptID, applicationName, user, containerID, hostname,
                    tags, pid, framework, component, executorId, mainClass);
        }
    }
}
