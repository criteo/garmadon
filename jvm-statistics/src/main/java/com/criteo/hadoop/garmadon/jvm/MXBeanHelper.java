package com.criteo.hadoop.garmadon.jvm;

public class MXBeanHelper {
    public static final String MEMORY_POOL_EDEN_HEADER = "eden";
    public static final String MEMORY_POOL_SURVIVOR_HEADER = "survivor";
    public static final String MEMORY_POOL_OLD_HEADER = "old";
    public static final String MEMORY_POOL_CODE_HEADER = "code";
    public static final String MEMORY_POOL_METASPACE_HEADER = "metaspace";
    public static final String MEMORY_POOL_COMPRESSEDCLASSPACE_HEADER = "compressedclassspace";
    public static final String MEMORY_POOL_PERM_HEADER = "perm";

    public static String normalizeName(String poolName)
    {
        if (poolName == null) return null;
        if (poolName.equals("Code Cache"))
        {
            return MEMORY_POOL_CODE_HEADER;
        }
        if (poolName.equals("Tenured Gen") || poolName.endsWith("Old Gen")  || poolName.equals("GenPauseless Old Gen"))
        {
            return MEMORY_POOL_OLD_HEADER;
        }
        if (poolName.endsWith("Perm Gen"))
        {
            return MEMORY_POOL_PERM_HEADER;
        }
        if (poolName.equals("Metaspace"))
        {
            return MEMORY_POOL_METASPACE_HEADER;
        }
        if (poolName.equals("Compressed Class Space"))
        {
            return MEMORY_POOL_COMPRESSEDCLASSPACE_HEADER;
        }
        if (poolName.endsWith("Eden Space") || poolName.equals("GenPauseless New Gen"))
        {
            return MEMORY_POOL_EDEN_HEADER;
        }
        if (poolName.endsWith("Survivor Space"))
        {
            return MEMORY_POOL_SURVIVOR_HEADER;
        }
        return poolName;
    }
}
