package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.AsyncEventProcessor;
import com.criteo.hadoop.garmadon.schema.enums.Component;
import com.criteo.hadoop.garmadon.schema.enums.Framework;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.function.Consumer;

/**
 * Abstract class for all modules related to containers
 * It's main purpose is to provide the header as raw bytes
 * Since we can get all information about the container
 * at the beginning, we gain serialization time
 */
public abstract class ContainerModule implements GarmadonAgentModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerModule.class);

    private final SerializedHeader header;

    private Framework framework = Framework.YARN;
    private Component component = Component.UNKNOWN;


    public ContainerModule() {
        setFrameworkComponent();
        this.header = createCachedHeader();
    }

    /**
     * Special header that is already serialized
     * Thus we gain perf just doing it once
     */
    public static class SerializedHeader extends Header {

        private final byte[] bytes;

        public SerializedHeader(byte[] bytes) {
            super(null, null, null, null, null, null,
                    null, null, null, null);
            this.bytes = bytes;
        }

        @Override
        public byte[] serialize() {
            return bytes;
        }
    }

    public Framework getFramework() {
        return framework;
    }

    public Component getComponent() {
        return component;
    }

    private void setFrameworkComponent() {
        String[] commands = System.getProperty("sun.java.command", "empty_class").split(" ");
        String mainClass = commands[0];
        switch (mainClass) {
            // MAP_REDUCE
            case "org.apache.hadoop.mapreduce.v2.app.MRAppMaster":
                framework = Framework.MAP_REDUCE;
                component = Component.APP_MASTER;
                break;
            case "org.apache.hadoop.mapred.YarnChild":
                framework = Framework.MAP_REDUCE;
                if (commands.length > 4) {
                    final TaskAttemptID firstTaskid = TaskAttemptID.forName(commands[3]);
                    try {
                        component = Component.valueOf(firstTaskid.getTaskType().name());
                    } catch (IllegalArgumentException ex) {
                        LOGGER.warn("Unknown component {}", firstTaskid.getTaskType().name());
                    }
                }
                break;
            // SPARK
            case "org.apache.spark.deploy.yarn.ApplicationMaster":
                framework = Framework.SPARK;
                component = Component.APP_MASTER;
                break;
            case "org.apache.spark.executor.CoarseGrainedExecutorBackend":
                framework = Framework.SPARK;
                component = Component.EXECUTOR;
                break;
            // FLINK
            case "org.apache.flink.yarn.YarnApplicationMasterRunner":
                framework = Framework.FLINK;
                component = Component.APP_MASTER;
                break;
            case "org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint":
                framework = Framework.FLINK;
                component = Component.APP_MASTER;
                break;
            case "org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint":
                framework = Framework.FLINK;
                component = Component.APP_MASTER;
                break;
            case "org.apache.flink.yarn.YarnTaskManager":
                framework = Framework.FLINK;
                component = Component.TASK_MANAGER;
                break;
            // Cuttle
            case "com.criteo.cuttle.contrib.yarn.ApplicationMaster":
                component = Component.CUTTLE_APP_MASTER;
                break;
            // YARN
            default:
                break;
        }
    }

    private SerializedHeader createCachedHeader() {
        String appName = "";
        String user = System.getenv(ApplicationConstants.Environment.USER.name());
        String containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        String host = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
        String pid = "UNKNOWN";
        try {
            pid = new File("/proc/self").getCanonicalFile().getName();
        } catch (IOException ignored) {
        }

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
                .withPid(pid)
                .withFramework(framework.name())
                .withComponent(component.name())
                .build()
                .serialize();
        return new SerializedHeader(bytes);
    }

    @Override
    public final void setup(Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        setup0(instrumentation, event -> eventProcessor.offer(header, event));
    }

    protected abstract void setup0(Instrumentation instrumentation, Consumer<Object> eventConsumer);
}
