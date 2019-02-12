package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.agent.modules.GarmadonAgentModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/**
 * Garmadon uses a simple blocking queue to allow threads from the running application
 * to produce events in a non blocking fashion via a call to the offer method.
 * Garmadon is based on ByteBuddy to instrument classes and intercept interesting part of the code
 * where we want to trace information.
 * <p>
 * The agent can be disabled if need by setting the {@code -Dgarmadon.disable} on the command line
 * <p>
 * We also define the notion of module which basically allows to setup specific context information
 * and instrumentation methods
 * For now, the agent does not close properly the event processor
 * (it only relies on the fact that it uses a daemon thread).
 * This means that we can loose some trailing events when the
 * container is killed
 * <p>
 * By default it uses a local garmadon forwarder.
 * It's possible to configure the agent to use consul discovery with 2 system properties:
 *
 * <ul>
 *     <li>{@code -Dgarmadon.discovery=consul}</li>
 *     <li>{@code -Dgarmadon.consul.service=SERVICE_NAME}</li>
 * </ul>
 *
 */
public class EventAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAgent.class);

    private static final String RELEASE = Optional
            .ofNullable(EventAgent.class.getPackage().getImplementationVersion()).orElse("1.0-SNAPSHOT");

    private final static int DEFAULT_FORWARDER_PORT = 31000;

    protected EventAgent() {
        throw new UnsupportedOperationException();
    }

    /**
     * Premain of Garmadon agent
     * <p>
     * It initializes the socket appender, the AsyncEventProcessor
     * (which is the thread serializing and pushing events to the appender),
     * the ContainerHeader gathering information about the running container,
     * attaches the instrumentation code and starts the thread reading JVM JMX events.
     * </p>
     *
     * @param arguments agent option
     * @param instrumentation agent instrumentation
     */
    public static void premain(String arguments, Instrumentation instrumentation) {
        try {
            if (System.getProperty("garmadon.disable") == null) {
                LOGGER.info("Starting Garmadon Agent Version {}", RELEASE);

                String discovery = System.getProperty("garmadon.discovery", "local");
                Connection connection = null;
                switch (discovery) {
                    case "local":
                        connection = new FixedConnection("127.0.0.1", DEFAULT_FORWARDER_PORT);
                        break;
                    case "consul":
                        String consulServiceName = System.getProperty("garmadon.consul.service");
                        connection = new ConsulConnection(consulServiceName);
                        break;
                    default:
                        throw new UnsupportedOperationException("discovery " + discovery + " is not supported yet");
                }

                SocketAppender appender = new SocketAppender(connection);

                // Init SocketAppender and EventProcessor
                AsyncEventProcessor eventProcessor = new AsyncEventProcessor(appender);

                //load user provided modules
                loadModules(arguments, instrumentation, eventProcessor);
                LOGGER.debug("Garmadon Agent initialized");

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void loadModules(String modules, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        if (modules == null) return;
        String[] classes = modules.split(",");
        for (String className : classes) {
            try {
                Class<?> clazz = Class.forName(className);
                Constructor<?> constructor = clazz.getConstructor();
                Object o = constructor.newInstance();
                if (o instanceof GarmadonAgentModule) {
                    LOGGER.debug("Setting up module {}", className);
                    GarmadonAgentModule module = (GarmadonAgentModule) o;
                    module.setup(instrumentation, eventProcessor);
                } else {
                    LOGGER.debug("module {} should implement GarmadonAgentModule. Skipping...", className);
                }
            } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                LOGGER.debug("module {} should define public NoArg constructor", className);
            } catch (ClassNotFoundException e) {
                LOGGER.debug("module {} could not be found on the classpath", className);
            }
        }
    }
}
