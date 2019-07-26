package com.criteo.hadoop.garmadon.jvm;

import org.junit.Assert;
import org.junit.Test;

public class StatisticsLogTest {

    @Test
    public void singlePropertyOnly() {
        StatisticsLog statisticsLog = new StatisticsLog();
        statisticsLog.add("foo", "bar");
        Assert.assertEquals("foo=bar", statisticsLog.flush());
    }

    @Test
    public void emptySection() {
        StatisticsLog statisticsLog = new StatisticsLog();
        statisticsLog.beginSection("foo");
        statisticsLog.endSection();
        Assert.assertEquals("", statisticsLog.flush());
    }

    @Test
    public void sectionOneProperty() {
        StatisticsLog statisticsLog = new StatisticsLog();
        statisticsLog.beginSection("foo");
        statisticsLog.add("name", "value");
        statisticsLog.endSection();
        Assert.assertEquals("foo[name=value]", statisticsLog.flush());
    }

    @Test
    public void properties() {
        StatisticsLog statisticsLog = new StatisticsLog();
        statisticsLog.beginSection("section");
        statisticsLog.add("strName", "strValue");
        statisticsLog.add("intName", Integer.MAX_VALUE);
        statisticsLog.add("longName", Long.MAX_VALUE);
        statisticsLog.addDuration("durationName", 42);
        statisticsLog.addPercentage("percentageName", 100);
        statisticsLog.addSize("sizeName", 1025);
        statisticsLog.endSection();
        Assert.assertEquals("section[strName=strValue, intName=2147483647, longName=9223372036854775807, durationName=42, %percentageName=100.0, sizeName=1]", statisticsLog.flush());
        // flush should have call reset
        Assert.assertEquals("", statisticsLog.flush());
    }
}