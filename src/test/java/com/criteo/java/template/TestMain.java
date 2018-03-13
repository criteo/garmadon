package com.criteo.java.template;

import java.util.Optional;

import org.junit.Test;
import org.junit.Assert;

public class TestMain {

    @Test
    public void testGreet() {
        String greeting = Main.greet(Optional.of("toto"));
        Assert.assertEquals(greeting, "Hello toto");
    }

}
