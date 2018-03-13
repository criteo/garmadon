package com.criteo.hadoop.garmadon.agent.utils;

import java.util.Random;

public class ObjectBuilderTestHelper {

    private static Random random = new Random();

    public static byte[] randomByteArray(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }
}
