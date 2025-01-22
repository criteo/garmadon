package com.criteo.hadoop.garmadon.jvm.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SparkRuntime {
    public static String getVersion() throws RuntimeException {
        if (VERSION.versionNumber == null) {
            throw new RuntimeException("Could not find Spark version. Is this a Spark application?", VERSION.throwable);
        }
        return VERSION.versionNumber;
    }

    private static final Version VERSION = computeVersion();

    static Version computeVersion() {
        try {
            return computeSpark32Version();
        } catch (Throwable t) {
            try {
                return computeSpark35Version();
            } catch (Throwable t2) {
                return new Version(t2);
            }
        }
    }

    private static Version computeSpark32Version() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
            Class<?> clazz = Class.forName("org.apache.spark.package$SparkBuildInfo$");
            Field moduleFIeld = clazz.getField("MODULE$");
            Object instance = moduleFIeld.get(null);
            Field versionField = clazz.getDeclaredField("spark_version");
            versionField.setAccessible(true);
            String version = (String) versionField.get(instance);
            return new Version(version);
    }

    private static Version computeSpark35Version() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            Class<?> clazz = Class.forName("org.apache.spark.SparkBuildInfo");
            Method versionMethod = clazz.getDeclaredMethod("spark_version");
            String version = (String) versionMethod.invoke(null);
            return new Version(version);
    }

    static class Version {
        final String versionNumber;
        final Throwable throwable;

        private Version(String versionNumber) {
            this.versionNumber = versionNumber;
            this.throwable = null;
        }

        private Version(Throwable throwable) {
            this.versionNumber = null;
            this.throwable = throwable;
        }
    }
}
