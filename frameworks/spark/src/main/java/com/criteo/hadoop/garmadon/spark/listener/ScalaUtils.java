package com.criteo.hadoop.garmadon.spark.listener;

import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.util.function.Supplier;

public class ScalaUtils {

    static final Function0<Long> ZERO_LONG_SUPPLIER = function0(() -> 0L);
    static final Function0<Long> CURRENT_TIME_MILLIS_SUPPLIER = function0(System::currentTimeMillis);
    static final Function0<String> EMPTY_STRING_SUPPLIER = function0(() -> "");

    protected ScalaUtils() {
        throw new UnsupportedOperationException();
    }

    static <T> Function0<T> function0(Supplier<T> s) {
        return new AbstractFunction0<T>() {
            @Override
            public T apply() {
                return s.get();
            }
        };
    }

}
