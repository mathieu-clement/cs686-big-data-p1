package edu.usfca.cs.dfs.structures;

import edu.usfca.cs.dfs.Utils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.SortedSet;
import java.util.TreeSet;

import static edu.usfca.cs.dfs.structures.Chunk.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChunkTest {
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
        Utils.writeStringToFile(inputFilename, inputString);

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
                for (byte b : sBytes) {
                    actualBytes[counter++] = b;
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

    @Test
    void testCreateFileFromChunks() throws Exception {
        int nbChunks = 4;
        SortedSet<Chunk> chunks = new TreeSet<>();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < nbChunks; ++i) {
            File chunkFile = File.createTempFile("chunktest", "chunk" + i);
            String content = "content" + i;
            Utils.writeStringToFile(chunkFile.getAbsolutePath(), content);
            Chunk chunk = new Chunk("theFilename", i, content.getBytes().length, Utils.md5sum(chunkFile), chunkFile.toPath());
            chunks.add(chunk);
            sb.append(content);
        }

        File outputTempFile = File.createTempFile("chunktest", "outputfile");
        File outputFileFromMethod = Chunk.createFileFromChunks(chunks, outputTempFile.getAbsolutePath());
        String actual = new String(Files.readAllBytes(outputFileFromMethod.toPath()));
        String expected = sb.toString();

        // Cleanup
        for (Chunk chunk : chunks) {
            chunk.getChunkLocalPath().toFile().delete();
        }
        outputTempFile.delete();

        assertEquals(expected, actual);
    }

    private void deleteAllFilesFromDirectory(File outputDirectoryFile) {
        String[] entries = outputDirectoryFile.list();
        for (String s : entries) {
            new File(outputDirectoryFile.getPath(), s).delete();
        }
    }
}