package com.criteo.hadoop.garmadon.agent.headers;

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

public class ContainerHeader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerHeader.class);
    private Header.SerializedHeader header;

    // Currently could not only rely on event from RM
    // as grafana/ES can't join on different event for display HDFS call per framework/component
    // or compute used per framework/component
    private Framework framework = Framework.YARN;
    private Component component = Component.UNKNOWN;
    private String executorId;
    private String mainClass;

    private void setFrameworkComponent() {
        String[] commands = Utils.getArrayJavaCommandLine();
        mainClass = commands[0];
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
                        LOGGER.debug("Unknown component {}", firstTaskid.getTaskType().name());
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
                try {
                    for (int i = 1; i < commands.length; i++) {
                        if (commands[i].equals("--executor-id")) {
                            executorId = commands[i + 1];
                            break;
                        }
                    }
                } catch (Exception e) {
                    LOGGER.debug("Failed to get executor id from command line", e);
                }
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
            // YARN
            default:
                break;
        }
    }

    private Header.SerializedHeader createCachedHeader() {
        String user = System.getenv(ApplicationConstants.Environment.USER.name());
        String containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        String host = System.getenv(ApplicationConstants.Environment.NM_HOST.name());

        // Get applicationID
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        ApplicationId appId = appAttemptID.getApplicationId();

        //build the header for the whole application once
        return Header.newBuilder()
                .withId(appId.toString())
                .addTag(Header.Tag.YARN_APPLICATION.name())
                .withHostname(host)
                .withApplicationID(appId.toString())
                .withAppAttemptID(appAttemptID.toString())
                .withUser(user)
                .withContainerID(containerIdString)
                .withPid(Utils.getPid())
                .withFramework(framework.name())
                .withComponent(component.name())
                .withExecutorId(executorId)
                .withMainClass(mainClass)
                .buildSerializedHeader();
    }

    private ContainerHeader() {
        setFrameworkComponent();
        this.header = createCachedHeader();
    }

    private static class SingletonHolder {
        private final static ContainerHeader instance = new ContainerHeader();
    }

    public static ContainerHeader getInstance() {
        return ContainerHeader.SingletonHolder.instance;
    }

    public Header.SerializedHeader getHeader() {
        return header;
    }

}
