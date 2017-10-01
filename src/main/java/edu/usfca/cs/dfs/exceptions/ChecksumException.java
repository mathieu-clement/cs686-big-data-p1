package edu.usfca.cs.dfs.exceptions;

import java.io.File;

public class ChecksumException extends RuntimeException {
    private String expectedChecksum;
    private String actualChecksum;
    private File file;

    public ChecksumException(File file, String expectedChecksum, String actualChecksum) {
        super("Checksum for file " + file + " is " + actualChecksum + " but expected " + expectedChecksum);
    }

    public String getExpectedChecksum() {
        return expectedChecksum;
    }

    public String getActualChecksum() {
        return actualChecksum;
    }

    public File getFile() {
        return file;
    }
}
