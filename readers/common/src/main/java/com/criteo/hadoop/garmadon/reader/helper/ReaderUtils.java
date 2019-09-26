package com.criteo.hadoop.garmadon.reader.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ReaderUtils {
    public static final Runnable EMPTY_ACTION = () -> {
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderUtils.class);

    protected ReaderUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T> T retryAction(Callable<T> action, String exceptionStr, Runnable actionFailure) {
        return retryAction(action, exceptionStr, actionFailure, 1000L);
    }

    public static <T> T retryAction(Callable<T> action, String exceptionStr, Runnable actionFailure, Long sleepDuration) {
        final int maxAttempts = 5;

        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                return action.call();
            } catch (Exception e) {
                String exMsg = String.format("%s. Retry (%d/%d)", exceptionStr, retry, maxAttempts);
                if (retry < maxAttempts) {
                    LOGGER.warn(exMsg, e);
                    try {
                        Thread.sleep(sleepDuration * retry);
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    LOGGER.error(exMsg, e);
                }
                actionFailure.run();
            }
        }
        throw new RuntimeException(exceptionStr);
    }
}
