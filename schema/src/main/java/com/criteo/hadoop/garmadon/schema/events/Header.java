package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Header {
    private final String id;
    private final String applicationID;
    private final String attemptID;
    private final String applicationName;
    private final String user;
    private final String containerID;
    private final String hostname;
    private final String pid;
    private final String framework;
    private final String frameworkVersion;
    private final String component;
    private final String executorId;
    private final String mainClass;
    private final String javaVersion;
    private final int javaFeature;
    private final List<String> tags;

    public enum Tag {
        YARN_APPLICATION,
        FORWARDER,
        RESOURCEMANAGER,
        NODEMANAGER,
        STANDALONE
    }

    private Header(Builder builder) {
        this.id = builder.id;
        this.applicationID = builder.applicationID;
        this.attemptID = builder.attemptID;
        this.applicationName = builder.applicationName;
        this.user = builder.user;
        this.containerID = builder.containerID;
        this.hostname = builder.hostname;
        this.tags = builder.tags;
        this.pid = builder.pid;
        this.framework = builder.framework;
        this.frameworkVersion = builder.frameworkVersion;
        this.component = builder.component;
        this.executorId = builder.executorId;
        this.mainClass = builder.mainClass;
        this.javaVersion = builder.javaVersion;
        this.javaFeature = builder.javaFeature;
    }

    public String getId() {
        return id;
    }

    public String getApplicationID() {
        return applicationID;
    }

    public String getAttemptID() {
        return attemptID;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getUser() {
        return user;
    }

    public String getContainerID() {
        return containerID;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPid() {
        return pid;
    }

    public String getFramework() {
        return framework;
    }

    public String getFrameworkVersion() {
        return frameworkVersion;
    }


    public String getComponent() {
        return component;
    }

    public String getExecutorId() {
        return executorId;
    }

    public String getMainClass() {
        return mainClass;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public int getJavaFeature() {
        return javaFeature;
    }

    public List<String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Header)) return false;

        Header otherHeader = (Header) other;

        return Objects.equals(this.id, otherHeader.id) &&
                Objects.equals(otherHeader.attemptID, this.attemptID) &&
                Objects.equals(this.applicationID, otherHeader.applicationID) &&
                Objects.equals(this.attemptID, otherHeader.attemptID) &&
                Objects.equals(this.applicationName, otherHeader.applicationName) &&
                Objects.equals(this.user, otherHeader.user) &&
                Objects.equals(this.containerID, otherHeader.containerID) &&
                Objects.equals(this.hostname, otherHeader.hostname) &&
                Objects.equals(this.tags, otherHeader.tags) &&
                Objects.equals(this.pid, otherHeader.pid) &&
                Objects.equals(this.framework, otherHeader.framework) &&
                Objects.equals(this.frameworkVersion, otherHeader.frameworkVersion) &&
                Objects.equals(this.component, otherHeader.component) &&
                Objects.equals(this.executorId, otherHeader.executorId) &&
                Objects.equals(this.mainClass, otherHeader.mainClass) &&
                Objects.equals(this.javaVersion, otherHeader.javaVersion) &&
                this.javaFeature == otherHeader.javaFeature;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, applicationID, attemptID, applicationName, user, containerID, hostname, pid, framework,
                frameworkVersion, component, executorId, mainClass, tags, javaVersion, javaFeature);
    }

    public Header cloneAndOverride(Header override) {
        Builder cloneBuilder = new Builder();
        cloneBuilder.id = override.id != null ? override.id : this.id;
        cloneBuilder.applicationID = override.applicationID != null ? override.applicationID : this.applicationID;
        cloneBuilder.attemptID = override.attemptID != null ? override.attemptID : this.attemptID;
        cloneBuilder.applicationName = override.applicationName != null ? override.applicationName : this.applicationName;
        cloneBuilder.user = override.user != null ? override.user : this.user;
        cloneBuilder.containerID = override.containerID != null ? override.containerID : this.containerID;
        cloneBuilder.hostname = override.hostname != null ? override.hostname : this.hostname;
        cloneBuilder.tags = (override.tags != null && !override.tags.isEmpty()) ? override.tags : this.tags;
        cloneBuilder.pid = override.pid != null ? override.pid : this.pid;
        cloneBuilder.framework = override.framework != null ? override.framework : this.framework;
        cloneBuilder.frameworkVersion = override.frameworkVersion != null ? override.frameworkVersion : this.frameworkVersion;
        cloneBuilder.component = override.component != null ? override.component : this.component;
        cloneBuilder.executorId = override.executorId != null ? override.executorId : this.executorId;
        cloneBuilder.mainClass = override.mainClass != null ? override.mainClass : this.mainClass;
        cloneBuilder.javaVersion = override.javaVersion != null ? override.javaVersion : this.javaVersion;
        cloneBuilder.javaFeature = override.javaFeature != 0 ? override.javaFeature : this.javaFeature;
        return cloneBuilder.build();
    }

    private Builder toBuilder() {
        Builder builder = new Builder();
        builder.id = this.id;
        builder.applicationID = this.applicationID;
        builder.attemptID = this.attemptID;
        builder.applicationName = this.applicationName;
        builder.user = this.user;
        builder.containerID = this.containerID;
        builder.hostname = this.hostname;
        builder.tags = this.tags;
        builder.pid = this.pid;
        builder.framework = this.framework;
        builder.frameworkVersion = this.frameworkVersion;
        builder.component = this.component;
        builder.executorId = this.executorId;
        builder.mainClass = this.mainClass;
        builder.javaVersion = this.javaVersion;
        builder.javaFeature = this.javaFeature;
        return builder;
    }

    public SerializedHeader toSerializeHeader() {
        return new SerializedHeader(toBuilder());
    }

    public byte[] serialize() {
        EventHeaderProtos.Header.Builder builder = EventHeaderProtos.Header.newBuilder();
        if (id != null) builder.setId(id);
        if (applicationID != null) builder.setApplicationId(applicationID);
        if (attemptID != null) builder.setAttemptId(attemptID);
        if (applicationName != null) builder.setApplicationName(applicationName);
        if (user != null) builder.setUsername(user);
        if (containerID != null) builder.setContainerId(containerID);
        if (hostname != null) builder.setHostname(hostname);
        if (tags != null && !tags.isEmpty()) {
            for (String tag : tags) {
                builder.addTags(tag);
            }
        }
        if (pid != null) builder.setPid(pid);
        if (framework != null) builder.setFramework(framework);
        if (frameworkVersion != null) builder.setFrameworkVersion(frameworkVersion);
        if (component != null) builder.setComponent(component);
        if (executorId != null) builder.setExecutorId(executorId);
        if (mainClass != null) builder.setMainClass(mainClass);
        if (javaVersion != null) builder.setJavaVersion(javaVersion);
        if (javaFeature != 0) builder.setJavaFeature(javaFeature);
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return "Header{" +
                "id='" + id + '\'' +
                ", applicationID='" + applicationID + '\'' +
                ", attemptID='" + attemptID + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", user='" + user + '\'' +
                ", containerID='" + containerID + '\'' +
                ", hostname='" + hostname + '\'' +
                ", pid='" + pid + '\'' +
                ", framework='" + framework + '\'' +
                ", frameworkVersion='" + frameworkVersion + '\'' +
                ", component='" + component + '\'' +
                ", executorId='" + executorId + '\'' +
                ", mainClass='" + mainClass + '\'' +
                ", javaVersion='" + javaVersion + '\'' +
                ", javaFeature='" + javaFeature + '\'' +
                ", tags=" + tags +
                '}';
    }

    public static class SerializedHeader extends Header {
        private final byte[] bytes;

        SerializedHeader(Builder builder) {
            super(builder);
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
        private static final Logger LOGGER = LoggerFactory.getLogger(Builder.class);
        private static final String TAGS_REGEX = "^[a-zA-Z0-9_\\-\\.]*$";

        private String id;
        private String applicationID;
        private String attemptID;
        private String applicationName;
        private String user;
        private String containerID;
        private String hostname;
        private String pid;
        private String framework;
        private String frameworkVersion;
        private String component;
        private String executorId;
        private String mainClass;
        private String javaVersion;
        private int javaFeature;

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

        public Builder withAttemptID(String attemptID) {
            this.attemptID = attemptID;
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

        public Builder addTags(String tags) {
            if (tags != null) {
                Arrays.stream(tags.split(",")).forEach(tag -> {
                    if (tag.matches(TAGS_REGEX)) {
                        this.tags.add(tag.toUpperCase());
                    } else {
                        LOGGER.warn("Tag {} not added as it doesn't complies to authorized chars {}", tag, TAGS_REGEX);
                    }
                });
            }
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

        public Builder withFrameworkVersion(String frameworkVersion) {
            this.frameworkVersion = frameworkVersion;
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

        public Builder withJavaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
            return this;
        }

        public Builder withJavaFeature(int javaFeature) {
            this.javaFeature = javaFeature;
            return this;
        }

        public Header build() {
            return new Header(this);
        }

        public SerializedHeader buildSerializedHeader() {
            return new SerializedHeader(this);
        }
    }
}
