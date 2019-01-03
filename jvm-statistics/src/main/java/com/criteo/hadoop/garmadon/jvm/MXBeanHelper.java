package com.criteo.hadoop.garmadon.jvm;

public class MXBeanHelper {
    public static final String MEMORY_POOL_EDEN_HEADER = "eden";
    public static final String MEMORY_POOL_SURVIVOR_HEADER = "survivor";
    public static final String MEMORY_POOL_OLD_HEADER = "old";
    public static final String MEMORY_POOL_CODE_HEADER = "code";
    public static final String MEMORY_POOL_METASPACE_HEADER = "metaspace";
    public static final String MEMORY_POOL_COMPRESSEDCLASSPACE_HEADER = "compressedclassspace";
    public static final String MEMORY_POOL_PERM_HEADER = "perm";

    protected MXBeanHelper() {
        throw new UnsupportedOperationException();
    }

    public static String normalizeName(String poolName) {
        if (poolName == null) return null;
        if ("Code Cache".equals(poolName)) {
            return MEMORY_POOL_CODE_HEADER;
        }
        if ("Tenured Gen".equals(poolName) || poolName.endsWith("Old Gen") || "GenPauseless Old Gen".equals(poolName)) {
            return MEMORY_POOL_OLD_HEADER;
        }
        if (poolName.endsWith("Perm Gen")) {
            return MEMORY_POOL_PERM_HEADER;
        }
        if ("Metaspace".equals(poolName)) {
            return MEMORY_POOL_METASPACE_HEADER;
        }
        if ("Compressed Class Space".equals(poolName)) {
            return MEMORY_POOL_COMPRESSEDCLASSPACE_HEADER;
        }
        if (poolName.endsWith("Eden Space") || "GenPauseless New Gen".equals(poolName)) {
            return MEMORY_POOL_EDEN_HEADER;
        }
        if (poolName.endsWith("Survivor Space")) {
            return MEMORY_POOL_SURVIVOR_HEADER;
        }
        return poolName;
    }
}
