package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

class ProcessMessageRunnable implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ProcessMessageRunnable.class);
    private final Socket socket;
    private final Map<String, SortedSet<Chunk>> chunks;

    public ProcessMessageRunnable(Socket socket, Map<String, SortedSet<Chunk>> chunks) {
        this.socket = socket;
        this.chunks = chunks;
    }

    @Override
    public void run() {
        try {
            Messages.MessageWrapper msg = Messages.MessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());

            // Dispatch
            if (msg.hasStoreChunkMsg()) {
                processStoreChunkMsg(socket, msg);
            }
        } catch (IOException e) {
            logger.error("Error while parsing message or other IO error", e);
        }
    }

    private void processStoreChunkMsg(Socket socket, Messages.MessageWrapper msgWrapper) throws IOException {
        Messages.StoreChunk storeChunkMsg
                = msgWrapper.getStoreChunkMsg();
        logger.debug("Storing file name: "
                + storeChunkMsg.getFileName() + " Chunk #" + storeChunkMsg.getSequenceNo() + " received from " +
                socket.getRemoteSocketAddress().toString());

        String storageDirectory = DFSProperties.getInstance().getStorageNodeChunksDir();
        File storageDirectoryFile = new File(storageDirectory);
        if (!storageDirectoryFile.exists()) {
            if (!storageDirectoryFile.mkdir()) {
                System.err.println("Could not create storage directory.");
                System.exit(1);
            }
        }

        Path chunkFilePath = Paths.get(storageDirectory, storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo());
        File chunkFile = chunkFilePath.toFile();
        if (chunkFile.exists()) {
            throw new IllegalStateException("There is already a chunk file named " + chunkFile.getName());
        }
        logger.debug("Storing to file " + chunkFilePath);
        FileOutputStream fos = new FileOutputStream(chunkFile);
        storeChunkMsg.getData().writeTo(fos);
        fos.close();
        // TODO Check sum

        addToChunkList(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), storeChunkMsg.getChecksum(), chunkFilePath);
    }

    private void addToChunkList(String fileName, int sequenceNo, String checksum, Path chunkFilePath) throws IOException {
        Chunk chunk = new Chunk(fileName, sequenceNo, Files.size(chunkFilePath), checksum, chunkFilePath);
        if (chunks.get(fileName) == null) {
            chunks.put(fileName, new TreeSet<Chunk>());
        }
        chunks.get(fileName).add(chunk);
    }

}
