package edu.usfca.cs.dfs.structures;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import static edu.usfca.cs.dfs.structures.Chunk.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ChunkTest {
    @Test
    void testCalculateNumberOfChunks() {
        assertEquals(1, calculateNumberOfChunks(100, 100));
        assertEquals(2, calculateNumberOfChunks(100, 99));
        assertEquals(2, calculateNumberOfChunks(100, 50));
        assertEquals(3, calculateNumberOfChunks(100, 49));
        assertEquals(1, calculateNumberOfChunks(100, 101));
    }


    @Test
    void testCalculateLastChunkSize() {
        assertEquals(100, calculateLastChunkSize(1, 100, 100));
        assertEquals(1, calculateLastChunkSize(2, 99, 100));
        assertEquals(50, calculateLastChunkSize(2, 50, 100));
        assertEquals(2, calculateLastChunkSize(3, 49, 100));
        assertEquals(100, calculateLastChunkSize(1, 101, 100));
    }


    @Test
    void testCreateChunksFromFile() throws IOException {
        String inputFilename = "/tmp/input_file";
        String inputString = "Hello, my name is R2-D2.\n";
        writeStringToFile(inputFilename, inputString);

        String outputDirectory = "/tmp/chunks";
        File outputDirectoryFile = new File(outputDirectory);
        if (outputDirectoryFile.exists()) {
            // Failsafe in case somebody doesn't actually read this test and changes outputDirectory
            // to a directory with valuable files in it...
            throw new IllegalStateException("Directory " + outputDirectory + " already exists.");
        }
        outputDirectoryFile.mkdir();
        try {
            createChunksFromFile(inputFilename, 8 /*  bytes */, outputDirectory);

            byte[] actualBytes = new byte[inputString.getBytes().length];
            int counter = 0;
            for (String s : outputDirectoryFile.list()) {
                byte[] sBytes = readBytesFromFile(new File(outputDirectory, s));
                for (int i = 0; i < sBytes.length; i++) {
                    actualBytes[counter++] = sBytes[i];
                }
            }
            assertArrayEquals(inputString.getBytes(), actualBytes);
        } finally {
            deleteAllFilesFromDirectory(outputDirectoryFile);
            outputDirectoryFile.delete();
        }
    }

    private byte[] readBytesFromFile(File file) throws IOException {
        return Files.readAllBytes(file.toPath());
    }

    private void writeStringToFile(String filename, String str) throws IOException {
        FileWriter inputFileWriter = new FileWriter(filename);
        inputFileWriter.write(str);
        inputFileWriter.close();
    }

    private void deleteAllFilesFromDirectory(File outputDirectoryFile) {
        String[] entries = outputDirectoryFile.list();
        for (String s : entries) {
            new File(outputDirectoryFile.getPath(), s).delete();
        }
    }
}