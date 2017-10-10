package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class ChunkCorruptionMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ChunkCorruptionMonitor.class);
    private final ComponentAddress storageNode;
    private final Map<String, SortedSet<Chunk>> chunkMap;
    private final Lock chunkMapLock;
    private final ComponentAddress controllerAddr;

    public ChunkCorruptionMonitor(ComponentAddress storageNode, Map<String, SortedSet<Chunk>> chunkMap, Lock chunkMapLock, ComponentAddress controllerAddr) {
        this.storageNode = storageNode;
        this.chunkMap = chunkMap;
        this.chunkMapLock = chunkMapLock;
        this.controllerAddr = controllerAddr;
    }

    @Override
    public void run() {
        while (true) {
            chunkMapLock.lock();
            try {
                logger.debug("Checking for corrupted files...");
                for (SortedSet<Chunk> chunks : chunkMap.values()) {
                    List<Chunk> corruptedChunks = new ArrayList<>();
                    for (Chunk chunk : chunks) {
                        if (chunk.isCorrupted()) {
                            logger.warn("Chunk " + chunk + " is corrupted.");
                            corruptedChunks.add(chunk);
                        }
                    }
                    for (Chunk chunk : corruptedChunks) {
                        File chunkFile = chunk.getChunkLocalPath().toFile();
                        File checksumFile = new File(chunkFile.getAbsolutePath() + ".md5");
                        chunkFile.delete();
                        checksumFile.delete();
                        chunks.remove(chunk);
                        try {
                            notifyChunkCorrupted(chunk);
                        } catch (IOException e) {
                            logger.error("Unable to connect to controller");
                        }
                    }
                }
            } finally {
                chunkMapLock.unlock();
            }

            // We might have removed all known chunks of a file
            for (String filename : new HashSet<>(chunkMap.keySet())) {
                if (chunkMap.get(filename).isEmpty()) {
                    chunkMap.remove(filename);
                }
            }

            try {
                Thread.sleep(DFSProperties.getInstance().getCorruptionVerificationPeriod());
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            }
        }
    }

    private void notifyChunkCorrupted(Chunk chunk) throws IOException {
        Messages.MessageWrapper msg = Messages.MessageWrapper.newBuilder()
                .setChunkCorruptedMsg(
                        Messages.ChunkCorrupted.newBuilder()
                                .setFilename(chunk.getFilename())
                                .setSequenceNo(chunk.getSequenceNo())
                                .setStorageNode(
                                        Messages.StorageNode.newBuilder()
                                                .setHost(storageNode.getHost())
                                                .setPort(storageNode.getPort())
                                                .build())
                )
                .build();
        Socket socket = controllerAddr.getSocket();
        msg.writeDelimitedTo(socket.getOutputStream());
        socket.close();
    }
}
