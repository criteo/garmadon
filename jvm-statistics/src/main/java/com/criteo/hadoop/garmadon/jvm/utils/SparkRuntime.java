package com.criteo.hadoop.garmadon.jvm.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class SparkRuntime {
    private static final String VERSION = computeVersion();

    private SparkRuntime() {
    }

    public static String getVersion() {
        return VERSION;
    }

    static String computeVersion() {
        try {
            return computeSpark32Version();
        } catch (Throwable e) {
            try {
                return computeSpark35Version();
            } catch (Throwable t) {
                return "unknown";
            }
        }
    }

    private static String computeSpark32Version() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = Class.forName("org.apache.spark.package$SparkBuildInfo$");
        Field moduleFIeld = clazz.getField("MODULE$");
        Object instance = moduleFIeld.get(null);
        Field versionField = clazz.getDeclaredField("spark_version");
        versionField.setAccessible(true);
        return (String) versionField.get(instance);
    }

    private static String computeSpark35Version() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = Class.forName("org.apache.spark.SparkBuildInfo");
        Method versionMethod = clazz.getDeclaredMethod("spark_version");
        return (String) versionMethod.invoke(null);
    }
}
