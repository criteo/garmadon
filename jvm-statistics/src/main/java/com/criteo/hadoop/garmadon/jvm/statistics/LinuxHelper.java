package com.criteo.hadoop.garmadon.jvm.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinuxHelper {
    public static final Pattern COUNT_PATTERN = Pattern.compile(":\\s+(\\d+)");
    private static final Pattern SPACES_PATTERN = Pattern.compile("\\s+");

    protected LinuxHelper() {
        throw new UnsupportedOperationException();
    }

    public static long getPageSize() {
        File fileSmaps = new File("/proc/self/smaps");
        if (fileSmaps.exists() && fileSmaps.canRead()) {
            String value = getFileSingleLine(fileSmaps, "MMUPageSize");
            return Long.parseLong(value) * 1024;
        }
        return 4096;
    }

    public static String[] getFileLineSplit(RandomAccessFile raf) {
        try {
            raf.seek(0);
            String line = raf.readLine();
            if (line == null) return new String[0];
            return SPACES_PATTERN.split(line);
        } catch (IOException ignored) {
            return new String[0];
        }
    }

    public static RandomAccessFile openFile(File file) {
        try {
            return file.exists() && file.canRead() ? new RandomAccessFile(file, "r") : null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    static String getFileSingleLine(File file, String key) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(key + ": ")) {
                    Matcher matcher = COUNT_PATTERN.matcher(line);
                    if (matcher.find()) return matcher.group(1);
                }
            }
        } catch (IOException ignored) {
        }
        return null;
    }
}

