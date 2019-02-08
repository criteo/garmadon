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
    private final String component;
    private final String executorId;
    private final String mainClass;
    private final List<String> tags;

    public enum Tag {
        YARN_APPLICATION,
        FORWARDER,
        RESOURCEMANAGER,
        NODEMANAGER,
        STANDALONE
    }

    public Header(String id, String applicationID, String attemptID, String applicationName,
                  String user, String containerID, String hostname, List<String> tags, String pid,
                  String framework, String component, String executorId, String mainClass) {
        this.id = id;
        this.applicationID = applicationID;
        this.attemptID = attemptID;
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
                Objects.equals(this.component, otherHeader.component) &&
                Objects.equals(this.executorId, otherHeader.executorId) &&
                Objects.equals(this.mainClass, otherHeader.mainClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, applicationID, attemptID, applicationName, user, containerID, hostname, pid, framework, component,
                executorId, mainClass, tags);
    }

    public Header cloneAndOverride(Header override) {
        String idClone = override.id != null ? override.id : this.id;
        String applicationIDClone = override.applicationID != null ? override.applicationID : this.applicationID;
        String attemptIDClone = override.attemptID != null ? override.attemptID : this.attemptID;
        String applicationNameClone = override.applicationName != null ? override.applicationName : this.applicationName;
        String userClone = override.user != null ? override.user : this.user;
        String containerIDClone = override.containerID != null ? override.containerID : this.containerID;
        String hostnameClone = override.hostname != null ? override.hostname : this.hostname;
        List<String> tagsClone = (override.tags != null && override.tags.size() > 0) ? override.tags : this.tags;
        String pidClone = override.pid != null ? override.pid : this.pid;
        String frameworkClone = override.framework != null ? override.framework : this.framework;
        String componentClone = override.component != null ? override.component : this.component;
        String executorIdClone = override.executorId != null ? override.executorId : this.executorId;
        String mainClassClone = override.mainClass != null ? override.mainClass : this.mainClass;
        return new Header(
                idClone,
                applicationIDClone,
                attemptIDClone,
                applicationNameClone,
                userClone,
                containerIDClone,
                hostnameClone,
                tagsClone,
                pidClone,
                frameworkClone,
                componentClone,
                executorIdClone,
                mainClassClone
        );
    }

    public SerializedHeader toSerializeHeader() {
        return new SerializedHeader(id, applicationID, attemptID, applicationName, user, containerID, hostname, tags, pid, framework,
                component, executorId, mainClass);
    }

    public byte[] serialize() {
        EventHeaderProtos.Header.Builder builder = EventHeaderProtos.Header
                .newBuilder();
        if (id != null) builder.setId(id);
        if (applicationID != null) builder.setApplicationId(applicationID);
        if (attemptID != null) builder.setAttemptId(attemptID);
        if (applicationName != null) builder.setApplicationName(applicationName);
        if (user != null) builder.setUsername(user);
        if (containerID != null) builder.setContainerId(containerID);
        if (hostname != null) builder.setHostname(hostname);
        if (tags != null && tags.size() > 0) {
            for (String tag : tags) {
                builder.addTags(tag);
            }
        }
        if (pid != null) builder.setPid(pid);
        if (framework != null) builder.setFramework(framework);
        if (component != null) builder.setComponent(component);
        if (executorId != null) builder.setExecutorId(executorId);
        if (mainClass != null) builder.setMainClass(mainClass);
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
        private static final Logger LOGGER = LoggerFactory.getLogger(Header.Builder.class);
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
            return new Header(id, applicationID, attemptID, applicationName, user, containerID, hostname, tags,
                    pid, framework, component, executorId, mainClass);
        }

        public SerializedHeader buildSerializedHeader() {
            return new SerializedHeader(id, applicationID, attemptID, applicationName, user, containerID, hostname,
                    tags, pid, framework, component, executorId, mainClass);
        }
    }
}
