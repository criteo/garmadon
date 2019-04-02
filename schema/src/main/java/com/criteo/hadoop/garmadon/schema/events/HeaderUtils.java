package com.criteo.hadoop.garmadon.schema.events;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class HeaderUtils {

    protected HeaderUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getHostname() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ignored) {
        }
        return hostname;
    }

    public static String getUser() {
        return System.getProperty("user.name", "");
    }

    public static String getPid() {
        String pid = "UNKNOWN";
        try {
            pid = new File("/proc/self").getCanonicalFile().getName();
        } catch (IOException ignored) {
        }
        return pid;
    }

    public static String getStandaloneId() {
        return getHostname() + ":" + getUser() + ":" + getPid();
    }

    public static String[] getArrayJavaCommandLine() {
        return System.getProperty("sun.java.command", "empty_class").split(" ");
    }

    public static String getJavaMainClass() {
        return getArrayJavaCommandLine()[0];
    }

}
