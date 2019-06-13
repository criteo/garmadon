package com.criteo.hadoop.garmadon.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class FileContextIntegrationTest {

    protected FileContextIntegrationTest() {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) throws IOException {
        FileContext fileContext = FileContext.getFileContext(new Configuration());

        Path src = new Path("/tmp/garmadon/test");
        Path dst = new Path("/tmp/garmadon/test2");

        fileContext.mkdir(src, FsPermission.getDefault(), true);

        fileContext.rename(src, dst, Options.Rename.OVERWRITE);

        fileContext.listStatus(dst);

        fileContext.delete(dst, false);
    }
}
