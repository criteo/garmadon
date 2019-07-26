package com.criteo.hadoop.garmadon.spark.listener;

import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.util.function.Supplier;

public class ScalaUtils {

    static Function0<Long> zeroLongSupplier = function0(() -> 0L);
    static Function0<Long> currentTimeMillisSupplier = function0(System::currentTimeMillis);
    static Function0<String> emptyStringSupplier = function0(() -> "");

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
