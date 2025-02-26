package com.criteo.hadoop.garmadon.jvm.utils;

import java.lang.reflect.Method;

public final class FlinkRuntime {
    private static final String VERSION = computeVersion();

    private FlinkRuntime() {
    }

    public static String getVersion() {
        return VERSION;
    }

    static String computeVersion() {
        try {
            Class<?> clazz = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation");
            Method versionMethod = clazz.getDeclaredMethod("getVersion");
            return (String) versionMethod.invoke(null);
        } catch (Throwable e) {
            return "unknown";
        }
    }
}
