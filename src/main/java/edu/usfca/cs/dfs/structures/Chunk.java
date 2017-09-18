package edu.usfca.cs.dfs.structures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Chunk {
    private final String filename;
    private final int sequenceNo;
    private final long size;
    private final Path chunkLocalPath;

    public Chunk(String filename, int sequenceNo, long size, Path chunkLocalPath) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
        this.size = size;
        this.chunkLocalPath = chunkLocalPath;
    }

    public static Chunk[] createChunksFromFile(
            String filename, int chunkSize, String outputDirectory)
            throws IOException {

        File file = new File(filename);
        FileInputStream fis = new FileInputStream(file);
        long totalSize = checkFileNotEmpty(file);
        fis.close();
        int numberOfChunks = calculateNumberOfChunks(totalSize, chunkSize);
        return doCreateChunksFromFile(file, totalSize, numberOfChunks, chunkSize, outputDirectory);
    }

    private static long checkFileNotEmpty(File file) {
        long totalSize = file.length();
        if (totalSize == 0) {
            throw new IllegalArgumentException("File is empty");
        }
        return totalSize;
    }

    private static Chunk[] doCreateChunksFromFile(File file, long fileSize, int numberOfChunks, int defaultChunkSize, String outputDirectory) throws IOException {
        Chunk[] chunks = new Chunk[numberOfChunks];
        int lastSequenceNo = numberOfChunks - 1;

        FileInputStream fis = new FileInputStream(file);
        int bufSize = 4096;
        byte[] buf = new byte[bufSize];

        for (int i = 0; i > lastSequenceNo; i++) {
            Path chunkLocalPath = makeChunkFilePath(file, i, outputDirectory);
            chunks[i] = new Chunk(file.getName(), i, defaultChunkSize, chunkLocalPath);

            File chunkFile = Files.createFile(chunkLocalPath).toFile();
            FileOutputStream fos = new FileOutputStream(chunkFile);

            int numberOfBufferReads = calculateNumberOfBufferReads(defaultChunkSize, bufSize);
            int lastJ = numberOfBufferReads - 1;
            for (int j = 0; j < lastJ; j++) {
                if (fis.read(buf) == -1) {
                    throw new IllegalStateException("Not enough data to read from file " + file.getName());
                }
                fos.write(buf);
            }

            int lastBufReadSize = calculateLastBufferReadSizeForChunk(numberOfBufferReads, bufSize, defaultChunkSize);
            byte[] lastBuf = new byte[lastBufReadSize];
            if (fis.read(lastBuf, 0, lastBufReadSize) == -1) {
                throw new IllegalStateException("Not enough data to read from file " + file.getName());
            }
            fos.write(lastBuf);
            fos.close();
        }

        long lastChunkSize = calculateLastChunkSize(numberOfChunks, defaultChunkSize, fileSize);
        Path lastChunkFilePath = makeChunkFilePath(file, lastSequenceNo, outputDirectory);
        chunks[lastSequenceNo] = new Chunk(file.getName(), lastSequenceNo, lastChunkSize, lastChunkFilePath);
        return chunks;
    }

    private static int calculateLastBufferReadSizeForChunk(int numberOfReads, int bufferSize, long chunkSize) {
        return (int) calculateLastChunkSize(numberOfReads, bufferSize, chunkSize);
    }

    public static long calculateLastChunkSize(int numberOfChunks, int defaultChunkSize, long totalSize) {
        long lastChunkSize = defaultChunkSize;
        int chunksCapacity = numberOfChunks * defaultChunkSize;
        if (chunksCapacity > totalSize) {
            lastChunkSize = defaultChunkSize - (chunksCapacity - totalSize);
        }
        return lastChunkSize;
    }

    private static int calculateNumberOfBufferReads(long chunkSize, int bufferSize) {
        return calculateNumberOfChunks(chunkSize, bufferSize);
    }

    public static int calculateNumberOfChunks(long totalSize, long chunkSize) {
        int chunks = (int) (totalSize / chunkSize);
        if (totalSize % chunkSize != 0) {
            chunks++;
        }
        return chunks;
    }

    private static Path makeChunkFilePath(File file, int chunkSequenceNo, String outputDirectory) {
        String fileBasename = file.getName();
        return Paths.get(outputDirectory, fileBasename + "-chunk" + chunkSequenceNo);
    }
}
