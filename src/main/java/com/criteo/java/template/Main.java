package com.criteo.java.template;

import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main
{
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static String greet(Optional<String> name) {
        return "Hello "+ name.orElse("anonymous");
    }

    public static void main(String[] args) {
        logger.info(greet(Arrays.stream(args).findFirst()));
    }
}
