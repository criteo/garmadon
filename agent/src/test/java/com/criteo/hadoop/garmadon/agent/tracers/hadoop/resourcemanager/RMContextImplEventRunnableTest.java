package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RMContextImplEventRunnableTest {
    private RMContextImplEventRunnable rmContextImplEventRunnable;
    private ApplicationId applicationId;
    private TriConsumer eventHandler;
    private ArgumentCaptor<ResourceManagerEventProtos.ApplicationEvent> argument;


    @Before
    public void setUp() {
        RMContextImpl rmContext = mock(RMContextImpl.class);
        eventHandler = mock(TriConsumer.class);
        applicationId = mock(ApplicationId.class);
        when(applicationId.toString()).thenReturn("application_id_001");

        rmContextImplEventRunnable = new RMContextImplEventRunnable(rmContext, eventHandler);

        argument = ArgumentCaptor.forClass(ResourceManagerEventProtos.ApplicationEvent.class);
    }

    @Test
    public void RMContextImplEventRunnable_sendAppEvent_should_not_failed_for_null_component_in_rmapp() {
        RMApp rmApp = mockRMApp("container_id_001", null, "project", "workflow", FinalApplicationStatus.SUCCEEDED);
        rmContextImplEventRunnable.sendAppEvent(applicationId, rmApp);
        verify(eventHandler).accept(any(Long.class), any(Header.class), argument.capture());

        ResourceManagerEventProtos.ApplicationEvent appEvent = argument.getValue();

        assertThat(appEvent.getAmContainerId()).isEqualTo("container_id_001");
        assertThat(appEvent.getTrackingUrl()).isEqualTo("");

        assertThat(appEvent.getProjectName()).isEqualTo("project");
        assertThat(appEvent.getWorkflowName()).isEqualTo("workflow");

        assertThat(appEvent.getFinalStatus()).isEqualTo("SUCCEEDED");
    }

    @Test
    public void RMContextImplEventRunnable_sendAppEvent_without_FinalStatus_should_not_fail() {
        RMApp rmApp = mockRMApp("container_id_001", null, "project", "workflow", null);
        rmContextImplEventRunnable.sendAppEvent(applicationId, rmApp);
        verify(eventHandler).accept(any(Long.class), any(Header.class), argument.capture());

        ResourceManagerEventProtos.ApplicationEvent appEvent = argument.getValue();

        assertThat(appEvent.getAmContainerId()).isEqualTo("container_id_001");
        assertThat(appEvent.getTrackingUrl()).isEqualTo("");

        assertThat(appEvent.getProjectName()).isEqualTo("project");
        assertThat(appEvent.getWorkflowName()).isEqualTo("workflow");

        assertThat(appEvent.getFinalStatus()).isEqualTo("");
    }

    public RMApp mockRMApp(String containerId, String trackingUrl, String projectName, String workflow, FinalApplicationStatus finalApplicationStatus) {
        RMApp rmApp = mock(RMApp.class);
        when(rmApp.getUser()).thenReturn("user");
        when(rmApp.getName()).thenReturn("app_name");
        when(rmApp.getApplicationType()).thenReturn("spark");
        when(rmApp.getQueue()).thenReturn("queue");
        when(rmApp.getTrackingUrl()).thenReturn(trackingUrl);
        when(rmApp.getState()).thenReturn(RMAppState.RUNNING);

        RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
        when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
        when(rmApp.getApplicationTags()).thenReturn(
            new HashSet<>(Arrays.asList("simpleTag", "garmadon.project.name:" + projectName, "garmadon.workflow.name:" + workflow))
        );
        when(rmAppAttempt.getFinalApplicationStatus()).thenReturn(finalApplicationStatus);

        ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);
        when(rmAppAttempt.getAppAttemptId()).thenReturn(applicationAttemptId);
        when(applicationAttemptId.toString()).thenReturn("application_attempt_id_001");

        Container container = mock(Container.class);
        when(rmAppAttempt.getMasterContainer()).thenReturn(container);

        ContainerId cid = mock(ContainerId.class);
        when(container.getId()).thenReturn(cid);
        when(cid.toString()).thenReturn(containerId);

        return rmApp;
    }
}
