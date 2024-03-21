package com.criteo.hadoop.garmadon.jvm.utils;

public final class JavaRuntime {

    private static final Version VERSION = parseVersion(System.getProperty("java.version"));

    private JavaRuntime() {
    }

    public static int getVersion() throws RuntimeException {
        if (VERSION.versionNumber == -1) {
            throw new RuntimeException("Could not parse Java version.", VERSION.parsingError);
        }
        return VERSION.versionNumber;
    }

    static Version parseVersion(String version) {
        try {
            int versionNumber;
            if (version.startsWith("1.")) {
                versionNumber = Integer.parseInt(version.substring(2, 3));
            } else {
                int dot = version.indexOf(".");
                versionNumber = Integer.parseInt(version.substring(0, dot));
            }
            return new Version(versionNumber);
        } catch (RuntimeException e) {
            return new Version(e);
        }
    }

    static final class Version {

        private final int versionNumber;
        private final RuntimeException parsingError;

        private Version(int versionNumber) {
            this.versionNumber = versionNumber;
            this.parsingError = null;
        }

        private Version(RuntimeException parsingError) {
            this.versionNumber = -1;
            this.parsingError = parsingError;
        }

        public int getVersionNumber() {
            return versionNumber;
        }

        public RuntimeException getParsingError() {
            return parsingError;
        }
    }
}
