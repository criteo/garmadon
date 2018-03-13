package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

public class Header {

    protected final String applicationID;
    protected final String appAttemptID;
    protected final String applicationName;
    protected final String user;
    protected final String containerID;
    protected final String hostname;
    protected final String tag;

    public enum Tag {
        YARN_APPLICATION,
        FORWARDER,
        NODE_MANAGER
    }

    public Header(String applicationID, String appAttemptID, String applicationName,
                  String user, String containerID, String hostname, String tag) {
        this.applicationID = applicationID;
        this.appAttemptID = appAttemptID;
        this.applicationName = applicationName;
        this.user = user;
        this.containerID = containerID;
        this.hostname = hostname;
        this.tag = tag;
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
                '}';
    }

    /**
     * Header that can be cloned with overridden values
     */
    public static class BaseHeader extends Header {
        private BaseHeader(String applicationID, String appAttemptID, String applicationName, String user, String containerID, String hostname, String tag) {
            super(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag);
        }

        public Header cloneAndOverride(Header override) {
            String applicationID = override.applicationID != null ? override.applicationID : this.applicationID;
            String appAttemptID = override.appAttemptID != null ? override.appAttemptID : this.appAttemptID;
            String applicationName = override.applicationName != null ? override.applicationName : this.applicationName;
            String user = override.user != null ? override.user : this.user;
            String containerID = override.containerID != null ? override.containerID : this.containerID;
            String hostname = override.hostname != null ? override.hostname : this.hostname;
            String tag = override.tag != null ? override.tag : this.tag;
            return new Header(
                    applicationID,
                    appAttemptID,
                    applicationName,
                    user,
                    containerID,
                    hostname,
                    tag
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

        public Header build() {
            return new Header(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag);
        }

        public BaseHeader buildBaseHeader() {
            return new BaseHeader(applicationID, appAttemptID, applicationName, user, containerID, hostname, tag);
        }
    }
}
