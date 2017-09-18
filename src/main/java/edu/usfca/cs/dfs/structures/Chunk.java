package edu.usfca.cs.dfs.structures;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Chunk {
    private final String filename;
    private final int sequenceNo;
    private final long size;

    public Chunk(String filename, int sequenceNo, long size) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
        this.size = size;
    }

    public static Chunk[] createChunksFromFile(
            String filename, int chunkSize, String outputDirectory)
            throws IOException {

        File file = new File(filename);
        FileInputStream fis = new FileInputStream(file);
        try {
            long totalSize = file.length();
            int numberOfChunks = calculateNumberOfChunks(totalSize, chunkSize);
        } finally {
            fis.close();
        }

        return null; // TODO Implement
    }

    public static int calculateNumberOfChunks(long totalSize, long chunkSize) {
        int chunks = (int) (totalSize / chunkSize);
        if (totalSize % chunkSize != 0) {
            chunks++;
        }
        return chunks;
    }
}
