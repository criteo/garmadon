package com.criteo.hadoop.garmadon.agent.utils;

import java.lang.reflect.Field;

public class ReflectionHelper {

    public static void setField(Object o, Class<?> clazz, String filedName, Object fieldValue) throws NoSuchFieldException, IllegalAccessException {
        Field fieldConf = clazz.getDeclaredField(filedName);
        fieldConf.setAccessible(true);
        fieldConf.set(o, fieldValue);
    }
}
