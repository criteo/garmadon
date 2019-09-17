package com.criteo.hadoop.garmadon.reader.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderUtils {
    public static final Runnable EMPTY_ACTION = () -> {
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderUtils.class);

    protected ReaderUtils() {
        throw new UnsupportedOperationException();
    }

    public static boolean retryAction(Runnable action, String exceptionStr, Runnable actionFailure) {
        final int maxAttempts = 5;

        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                action.run();
                return true;
            } catch (Exception e) {
                String exMsg = String.format("%s. Retry (%d/%d)", exceptionStr, retry, maxAttempts);
                if (retry < maxAttempts) {
                    LOGGER.warn(exMsg, e);
                    try {
                        Thread.sleep(1000 * retry);
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
