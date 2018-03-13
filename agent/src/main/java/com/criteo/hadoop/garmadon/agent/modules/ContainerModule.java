package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.lang.instrument.Instrumentation;
import java.util.function.Consumer;

/**
 * Abstract class for all modules related to containers
 * It's main purpose is to provide the header as raw bytes
 * Since we can get all information about the container
 * at the beginning, we gain serialization time
 */
public abstract class ContainerModule implements GarmadonAgentModule {

    private final SerializedHeader header;

    public ContainerModule() {
        this.header = createCachedHeader();
    }

    /**
     * Special header that is already serialized
     * Thus we gain perf just doing it once
     */
    public static class SerializedHeader extends Header {

        private final byte[] bytes;

        public SerializedHeader(byte[] bytes) {
            super(null, null, null, null, null, null, null);
            this.bytes = bytes;
        }

        @Override
        public byte[] serialize() {
            return bytes;
        }
    }

    private SerializedHeader createCachedHeader() {
        String appName = "";
        String user = System.getenv(ApplicationConstants.Environment.USER.name());
        String containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        String host = System.getenv(ApplicationConstants.Environment.NM_HOST.name());

        // Get applicationID
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        ApplicationId appId = appAttemptID.getApplicationId();

        //build the header for the whole application once
        byte[] bytes = Header.newBuilder()
                .withTag(Header.Tag.YARN_APPLICATION.name())
                .withHostname(host)
                .withApplicationID(appId.toString())
                .withApplicationName(appName)
                .withAppAttemptID(appAttemptID.toString())
                .withUser(user)
                .withContainerID(containerIdString)
                .build()
                .serialize();
        return new SerializedHeader(bytes);
    }

    @Override
    public final void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor){
        setup0(instrumentation, event -> eventProcessor.offer(header, event));
    }

    protected abstract void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer);
}
