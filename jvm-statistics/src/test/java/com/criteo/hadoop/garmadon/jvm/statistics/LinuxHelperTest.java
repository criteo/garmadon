package com.criteo.hadoop.garmadon.jvm.statistics;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LinuxHelperTest {

    @Test
    public void getFileSingleLine() {
        File testFile = new File(LinuxHelperTest.class.getResource("/smaps.txt").getFile());
        Assert.assertEquals("4", LinuxHelper.getFileSingleLine(testFile, "MMUPageSize"));
    }

    @Test
    public void getFileLineSplit() throws IOException {
        String[] split;
        try (RandomAccessFile raf = new RandomAccessFile(LinuxHelperTest.class.getResource("/smaps.txt").getFile(), "r")) {
            split = LinuxHelper.getFileLineSplit(raf);
        }
        Assert.assertEquals(8, split.length);
        Assert.assertEquals("cpu", split[0]);
        Assert.assertEquals("12429873894", split[1]);
        Assert.assertEquals("112423", split[2]);
        Assert.assertEquals("2578676578", split[3]);
        Assert.assertEquals("95690623088", split[4]);
        Assert.assertEquals("77250562", split[5]);
        Assert.assertEquals("0", split[6]);
        Assert.assertEquals("357862258", split[7]);
    }
}