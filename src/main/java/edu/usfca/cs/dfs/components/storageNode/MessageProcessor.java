package edu.usfca.cs.dfs.components.storageNode;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.Utils;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import static edu.usfca.cs.dfs.Utils.checkSum;

class MessageProcessor implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private final Socket socket;
    private final Map<String, SortedSet<Chunk>> chunks;
    private final Map<ComponentAddress, Socket> storageNodeSockets = new HashMap<>();

    public MessageProcessor(Socket socket, Map<String, SortedSet<Chunk>> chunks) {
        logger.trace("Starting Message Processor, thread " + Thread.currentThread().getName());
        this.socket = socket;
        this.chunks = chunks;
    }

    @Override
    public void run() {
        int countExceptions = 0;
        int nullMessageCount = 0;

        while (!socket.isClosed()) {
            try {
                Messages.MessageWrapper msg = Messages.MessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());

                if (msg == null) {
                    nullMessageCount++;
                    logger.warn("Incoming null message");
                    if (nullMessageCount == 10) {
                        logger.error("Too many null messages. Closing socket");
                        socket.close();
                        return;
                    } else {
                        continue;
                    }
                }

                // Dispatch
                if (msg.hasStoreChunkMsg()) {
                    logger.trace("Incoming store chunk message");
                    processStoreChunkMsg(socket, msg);
                } else if (msg.hasOrderSendChunkMsg()) {
                    logger.trace("Incoming order send chunk message");
                    processOrderSendChunkMsg(msg);
                } else if (msg.hasDownloadChunkMsg()) {
                    logger.trace("Incoming download chunk message");
                    processDownloadChunkMsg(socket, msg);
                }
            } catch (IOException e) {
                logger.error("Error while parsing message or other IO error", e);
                countExceptions++;
                if (countExceptions == 10) {
                    logger.error("Something is very wrong here. Too many problems when reading messages. Exiting.");
                    System.exit(1);
                }
            }
        }
    }

    private void processDownloadChunkMsg(Socket socket, Messages.MessageWrapper messageWrapper) throws IOException {
        Messages.DownloadChunk msg = messageWrapper.getDownloadChunkMsg();
        String filename = msg.getFilename();
        int sequenceNo = msg.getSequenceNo();
        sendChunk(filename, sequenceNo, socket);
    }

    private void processOrderSendChunkMsg(Messages.MessageWrapper msgWrapper) throws IOException {
        Messages.OrderSendChunk msg = msgWrapper.getOrderSendChunkMsg();
        String host = msg.getStorageNode().getHost();
        int port = msg.getStorageNode().getPort();
        ComponentAddress storageNode = new ComponentAddress(host, port);
        String filename = msg.getFileChunk().getFilename();
        int sequenceNo = msg.getFileChunk().getSequenceNo();
        logger.debug("Controller wants me to send " + filename + "-chunk" + sequenceNo + " to " + storageNode);

        // Connect to that other storage socket
        if (storageNodeSockets.get(storageNode) == null || storageNodeSockets.get(storageNode).isClosed()) {
            storageNodeSockets.put(storageNode, storageNode.getSocket());
        }
        Socket socket = storageNodeSockets.get(storageNode);

        logger.debug("Sending to " + storageNode);
        sendChunk(filename, sequenceNo, socket);
    }

    private void sendChunk(String filename, int sequenceNo, Socket socket) throws IOException {
        // Retrieve the chunk on local filesystem
        String chunkFileName = filename + "-chunk" + sequenceNo;
        Path chunkPath = Paths.get(DFSProperties.getInstance().getStorageNodeChunksDir(), chunkFileName);
        File chunkFile = chunkPath.toFile();
        if (!chunkFile.exists()) {
            throw new IllegalStateException("I don't have " + chunkPath.toString() + ". Can't send it to another storage node.");
        }

        // send a store chunk message
        FileInputStream fis = new FileInputStream(chunkFile);
        String expectedChecksum = new String(Files.readAllBytes(Paths.get(DFSProperties.getInstance().getStorageNodeChunksDir(), chunkFileName + ".md5"))).split(" ")[0];
        Utils.checkSum(chunkFile, expectedChecksum); // TODO Tell controller if we messed up

        Messages.MessageWrapper msg = Messages.MessageWrapper.newBuilder()
                .setStoreChunkMsg(
                        Messages.StoreChunk.newBuilder()
                                .setFileName(filename)
                                .setSequenceNo(sequenceNo)
                                .setData(ByteString.readFrom(fis))
                                .setChecksum(expectedChecksum)
                                .build()
                ).build();
        fis.close();
        logger.debug("Sending " + chunkFileName + " to " + socket.getRemoteSocketAddress());
        msg.writeDelimitedTo(socket.getOutputStream());
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

        // Store chunk file
        String chunkFilename = storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo();
        Path chunkFilePath = Paths.get(storageDirectory, chunkFilename);
        File chunkFile = chunkFilePath.toFile();
        if (chunkFile.exists()) {
            if (!chunkFile.delete()) {
                throw new RuntimeException("Unable to delete existing file before overwriting");
            }
        }
        logger.debug("Storing to file " + chunkFilePath);
        FileOutputStream fos = new FileOutputStream(chunkFile);
        storeChunkMsg.getData().writeTo(fos);
        fos.close();

        // Store checksum
        checkSum(chunkFile, storeChunkMsg.getChecksum());
        Path checksumFilePath = Paths.get(storageDirectory, chunkFilename + ".md5");
        logger.debug("Storing checksum on disk to file " + checksumFilePath);
        Utils.writeStringToFile(checksumFilePath.toString(), storeChunkMsg.getChecksum() + "  " + chunkFilename + "\n");

        // Update program state
        addToChunkList(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), storeChunkMsg.getChecksum(), chunkFilePath);
    }

    private void addToChunkList(String fileName, int sequenceNo, String checksum, Path chunkFilePath) throws IOException {
        Chunk chunk = new Chunk(fileName, sequenceNo, Files.size(chunkFilePath), checksum, chunkFilePath);
        StorageNode.addToChunks(chunk, chunks);
    }
}
