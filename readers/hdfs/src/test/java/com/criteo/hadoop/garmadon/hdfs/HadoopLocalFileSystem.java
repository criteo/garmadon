package com.criteo.hadoop.garmadon.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import java.io.IOException;

public class HadoopLocalFileSystem {

    /*
     * Helper method to get HadoopLocalFileSystem and get around hive that sets
     * org.apache.hadoop.hive.ql.io.ProxyLocalFileSystem as default filesystem
     * for uri file://
     */

    public static FileSystem get(Configuration conf) throws IOException {
        conf.set("fs.file.impl", LocalFileSystem.class.getCanonicalName());
        conf.set("fs.file.impl.disable.cache", "true");
        return FileSystem.getLocal(conf);
    }


}
