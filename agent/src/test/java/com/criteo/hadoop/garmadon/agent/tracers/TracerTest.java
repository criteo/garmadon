package com.criteo.hadoop.garmadon.agent.tracers;

import net.bytebuddy.description.NamedElement;
import net.bytebuddy.matcher.ElementMatcher;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TracerTest {

    @Test
    public void Tracer_should_exclude_undesired_packages(){
        ElementMatcher<NamedElement> matcher = Tracer.ignoredMatcher;

        assertThat(matcher.matches(() -> "java."), is(true));
        assertThat(matcher.matches(() -> "org.mockito"), is(true));
        assertThat(matcher.matches(() -> "sun.net."), is(true));
        assertThat(matcher.matches(() -> "com.criteo.hadoop.garmadon."), is(true));
        assertThat(matcher.matches(() -> "com.criteo.jvm."), is(true));
    }

    @Test
    public void Tracer_should_include_desired_packages(){
        ElementMatcher<NamedElement> matcher = Tracer.ignoredMatcher;

        assertThat(matcher.matches(() -> "org.apache.hadoop."), is(false));
        assertThat(matcher.matches(() -> "org.apache.spark."), is(false));
        assertThat(matcher.matches(() -> "com.criteo."), is(false));
    }


}
