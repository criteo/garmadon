package com.criteo.hadoop.garmadon.agent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * Small logging class for better output than Sysout everywhere
 * The idea is to avoid to use any lib or rely on an slf4j impl
 * to be present on the classpath
 * <p>
 * In practice, there should always be some slf4j compliant logging lib
 * on the classpath so TODO: see if wee keep or get a slf4j?
 */
public class Logger {

    private Consumer<String> sysOutAppender = System.out::println;
    private Consumer<String> sysErrAppender = System.err::println;

    private enum LEVEL {
        DEBUG(0), INFO(1), WARN(2), ERROR(3);

        private final int level;

        LEVEL(int level) {
            this.level = level;
        }
    }

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static LEVEL logLevel;

    static {
        String sLevel = System.getProperty("garmadon.logLevel", "INFO");
        try {
            logLevel = LEVEL.valueOf(sLevel);
        } catch (IllegalArgumentException e) {
            //not a valid logging level, defaulting to INFO
            logLevel = LEVEL.valueOf("INFO");
        }
    }

    private final String name;

    private Logger(String name) {
        this.name = name;
    }

    public void debug(String message) {
        if (isDebugEnabled()) {
            formatAndFlush(LEVEL.DEBUG.name(), message, sysOutAppender);
        }
    }

    public void info(String message) {
        if (isInfoEnabled()) {
            formatAndFlush(LEVEL.INFO.name(), message, sysOutAppender);
        }
    }

    public void warn(String message) {
        if (isWarnEnabled()) {
            formatAndFlush(LEVEL.WARN.name(), message, sysOutAppender);
        }
    }

    public void error(String message) {
        if (isErrorEnabled()) {
            formatAndFlush(LEVEL.ERROR.name(), message, sysErrAppender);
        }
    }

    public void error(String message, Throwable t) {
        error(message + " - " + t.getClass().getName() + ": " + t.getMessage());
    }

    private boolean isDebugEnabled() {
        return logLevel.level == LEVEL.DEBUG.level;
    }

    private boolean isInfoEnabled() {
        return logLevel.level <= LEVEL.INFO.level;
    }

    private boolean isWarnEnabled() {
        return logLevel.level <= LEVEL.WARN.level;
    }

    private boolean isErrorEnabled() {
        return logLevel.level <= LEVEL.ERROR.level;
    }

    private void formatAndFlush(String level, String message, Consumer<String> appender) {
        StringBuilder sb = new StringBuilder();
        sb.append(LocalDateTime.now().format(dtf));
        sb.append(" - ");
        sb.append(level);
        sb.append(" - ");
        sb.append(Thread.currentThread().getName());
        sb.append(" - ");
        sb.append(name);
        sb.append(" - ");
        sb.append(message);

        appender.accept(sb.toString());
    }

    public static <T> Logger getLogger(Class<T> aClass) {
        return new Logger(aClass.getName());
    }
}
