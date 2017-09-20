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
            String filename, long chunkSize, String outputDirectory)
            throws IOException {

        File file = new File(filename);
        FileInputStream fis = new FileInputStream(file);
        long totalSize = checkFileNotEmpty(file);
        fis.close();
        int numberOfChunks = calculateNumberOfChunks(totalSize, chunkSize);
        return doCreateChunksFromFile(file, totalSize, numberOfChunks, chunkSize, outputDirectory);
    }

    public String getFilename() {
        return filename;
    }

    public int getSequenceNo() {
        return sequenceNo;
    }

    public long getSize() {
        return size;
    }

    public Path getChunkLocalPath() {
        return chunkLocalPath;
    }

    private static long checkFileNotEmpty(File file) {
        long totalSize = file.length();
        if (totalSize == 0) {
            throw new IllegalArgumentException("File is empty");
        }
        return totalSize;
    }

    private static Chunk[] doCreateChunksFromFile(File file, long fileSize, int numberOfChunks, long defaultChunkSize, String outputDirectory) throws IOException {
        Chunk[] chunks = new Chunk[numberOfChunks];
        int lastSequenceNo = numberOfChunks - 1;

        FileInputStream fis = new FileInputStream(file);

        for (int i = 0; i < lastSequenceNo; i++) {
            Path chunkLocalPath = makeChunkFilePath(file, i, outputDirectory);
            chunks[i] = new Chunk(file.getName(), i, defaultChunkSize, chunkLocalPath);
            writeToChunkFile(fis, chunkLocalPath, defaultChunkSize);
        }

        long lastChunkSize = calculateLastChunkSize(numberOfChunks, defaultChunkSize, fileSize);
        Path lastChunkFilePath = makeChunkFilePath(file, lastSequenceNo, outputDirectory);
        chunks[lastSequenceNo] = new Chunk(file.getName(), lastSequenceNo, lastChunkSize, lastChunkFilePath);
        writeToChunkFile(fis, lastChunkFilePath, lastChunkSize);
        return chunks;
    }

    private static void writeToChunkFile(FileInputStream fis, Path chunkFilePath, long chunkSize) throws IOException {
        File chunkFile = Files.createFile(chunkFilePath).toFile();
        FileOutputStream fos = new FileOutputStream(chunkFile);

        int bufSize = 4096;
        byte[] buf = new byte[bufSize];

        int numberOfBufferReads = calculateNumberOfBufferReads(chunkSize, bufSize);
        int lastJ = numberOfBufferReads - 1;
        for (int j = 0; j < lastJ; j++) {
            if (fis.read(buf) == -1) {
                throw new IllegalStateException("Not enough data to read from file");
            }
            fos.write(buf);
        }

        int lastBufReadSize = calculateLastBufferReadSizeForChunk(numberOfBufferReads, bufSize, chunkSize);
        byte[] lastBuf = new byte[lastBufReadSize];
        if (fis.read(lastBuf, 0, lastBufReadSize) == -1) {
            throw new IllegalStateException("Not enough data to read from file");
        }
        fos.write(lastBuf);
        fos.close();
    }

    private static int calculateLastBufferReadSizeForChunk(int numberOfReads, int bufferSize, long chunkSize) {
        return (int) calculateLastChunkSize(numberOfReads, bufferSize, chunkSize);
    }

    static long calculateLastChunkSize(int numberOfChunks, long defaultChunkSize, long totalSize) {
        long lastChunkSize = defaultChunkSize;
        long chunksCapacity = numberOfChunks * defaultChunkSize;
        if (chunksCapacity > totalSize) {
            lastChunkSize = defaultChunkSize - (chunksCapacity - totalSize);
        }
        return lastChunkSize;
    }

    private static int calculateNumberOfBufferReads(long chunkSize, int bufferSize) {
        return calculateNumberOfChunks(chunkSize, bufferSize);
    }

    static int calculateNumberOfChunks(long totalSize, long chunkSize) {
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
