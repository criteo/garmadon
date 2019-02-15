package com.criteo.hadoop.garmadon.heuristics.configurations;

public class HeuristicsConfiguration {
    private int fileMaxCreatedFiles = 1000000;

    public int getFileMaxCreatedFiles() {
        return fileMaxCreatedFiles;
    }

    public void setFileMaxCreatedFiles(int fileMaxCreatedFiles) {
        this.fileMaxCreatedFiles = fileMaxCreatedFiles;
    }
}
