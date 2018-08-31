package com.criteo.hadoop.garmadon.elasticsearch;

public class UriHelper {
    static String envDc = System.getenv("CRITEO_ENV") + "-" + System.getenv("CRITEO_DC");

    private static String concatHdfsUri() {
        return concatHdfsUri(null);
    }

    private static String concatHdfsUri(String prefix) {
        String concatUri = "hdfs://";
        if (prefix != null) {
            concatUri += prefix + "-";
        }
        concatUri += envDc;
        return concatUri;
    }

    public static String getUniformizedUri(String uri) {
        // Remove port from uri, for.ex get only hdfs://root from hdfs://root:8020
        String[] splittedUri = uri.split(":");
        if (splittedUri.length > 2) {
            uri = splittedUri[0] + ":" + splittedUri[1];
        }

        // Set environment dc uri
        String uniformizedUri;
        switch (uri) {
            case "hdfs://root":
                uniformizedUri = concatHdfsUri();
                break;
            case "hdfs://yarn":
                uniformizedUri = concatHdfsUri("yarn");
                break;
            case "hdfs://glup":
                uniformizedUri = concatHdfsUri("glup");
                break;
            default:
                uniformizedUri = uri;
                break;
        }
        return uniformizedUri;
    }

}