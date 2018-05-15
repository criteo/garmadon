package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.agent.modules.*;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Garmadon uses a simple blocking queue to allow threads from the running application
 * to produce events in a non blocking fashion via a call to the offer method.
 * Garmadon is based on ByteBuddy to instrument classes and intercept interesting part of the code
 * where we want to trace information.
 * It also uses Criteo's jvm statistics project https://gitlab.criteois.com/jp.bempel/JVMStatistics
 * to produce JVM JMX related events
 * <p>
 * The agent can be disabled if need by setting the -Dgarmadon.disable on the command line
 * <p>
 * We also define the notion of module which basically allows to setup specific context information
 * and instrumentation methods
 * For now, the agent does not close properly the event processor
 * (it only relies on the fact that it uses a daemon thread).
 * This means that we can loose some trailing events when the
 * container is killed
 */
public class EventAgent {

    private static final Logger logger = Logger.getLogger(EventAgent.class);

    private final static int DEFAULT_FORWARDER_PORT = 33000;

    /**
     * Premain of Garmadon agent
     * <p>
     * It initializes the socket appender, the AsyncEventProcessor
     * (which is the thread serializing and pushing events to the appender),
     * the ContainerHeader gathering information about the running container,
     * attaches the instrumentation code and starts the thread reading JVM JMX events.
     *
     * @param arguments
     * @param instrumentation
     */
    public static void premain(String arguments, Instrumentation instrumentation) {
        try {
            if (System.getProperty("garmadon.disable") == null) {
                logger.info("Garmadon Agent start");

                // Init SocketAppender and EventProcessor
                SocketAppender appender = new SocketAppender("127.0.0.1", DEFAULT_FORWARDER_PORT);
                AsyncEventProcessor eventProcessor = new AsyncEventProcessor(appender);

                //load user provided modules
                loadModules(arguments, instrumentation, eventProcessor);
                logger.info("Garmadon Agent initialized");

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void loadModules(String modules, Instrumentation instrumentation, AsyncEventProcessor eventProcessor) {
        if (modules == null)
            return;
        String[] classes = modules.split(",");
        for (String className : classes) {
            try {
                Class<?> clazz = Class.forName(className);
                Constructor<?> constructor = clazz.getConstructor();
                Object o = constructor.newInstance();
                if (o instanceof GarmadonAgentModule) {
                    logger.info("Setting up module " + className);
                    GarmadonAgentModule module = (GarmadonAgentModule) o;
                    module.setup(instrumentation, eventProcessor);
                } else {
                    logger.warn("module " + className + " should implement GarmadonAgentModule. Skipping...");
                }
            } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                logger.warn("module " + className + " should define public NoArg constructor");
            } catch (ClassNotFoundException e) {
                logger.warn("module " + className + " could not be found on the classpath");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting...");
        Thread.sleep(30 * 1000);
        System.out.println("stopping...");
    }
}
