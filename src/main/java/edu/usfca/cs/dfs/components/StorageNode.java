package edu.usfca.cs.dfs.components;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class StorageNode {

    private final static Logger logger = LoggerFactory.getLogger(StorageNode.class);

    private ServerSocket srvSocket;

    private final int port;
    private final ComponentAddress controllerAddr;

    // Map of chunks. Key is filename.
    private final Map<String, SortedSet<Chunk>> chunks = new HashMap<>();

    public StorageNode(int port, ComponentAddress controllerAddr) {
        this.port = port;
        this.controllerAddr = controllerAddr;
    }

    public static void main(String[] args)
            throws Exception {
        if (args.length != 3) {
            System.err.println("This program requires 3 arguments: storage-node-listening-port controller-address controller-listening-port");
            System.exit(1);
        }
        String hostname = getHostname();
        int port = Integer.parseInt(args[0]);
        String controllerHost = args[1];
        int controllerPort = Integer.parseInt(args[2]);
        ComponentAddress controllerAddr = new ComponentAddress(controllerHost, controllerPort);

        logger.info("Starting storage node on " + hostname + " port " + port + "...");
        logger.info("Will connect to controller " + controllerAddr);
        new StorageNode(port, controllerAddr).start();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    public void start()
            throws Exception {
        new Thread(new HeartbeatRunnable(new ComponentAddress(getHostname(), port), controllerAddr, chunks)).start();

        srvSocket = new ServerSocket(port);
        logger.debug("Listening on port " + port + "...");
        while (true) {
            Socket socket = srvSocket.accept();
            Messages.MessageWrapper msgWrapper
                    = Messages.MessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());

            if (msgWrapper.hasStoreChunkMsg()) {
                Messages.StoreChunk storeChunkMsg
                        = msgWrapper.getStoreChunkMsg();
                logger.debug("Storing file name: "
                        + storeChunkMsg.getFileName() + " Chunk #" + storeChunkMsg.getSequenceNo() + " received from " +
                        socket.getRemoteSocketAddress().toString());

                String storageDirectory = "/tmp/storage-node-" + port;
                File storageDirectoryFile = new File(storageDirectory);
                if (!storageDirectoryFile.exists()) {
                    storageDirectoryFile.mkdir();
                }

                Path chunkFilePath = Paths.get(storageDirectory, storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo());
                // TODO Check if chunk is already stored on this node
                logger.debug("Storing to file " + chunkFilePath);
                FileOutputStream fos = new FileOutputStream(chunkFilePath.toFile());
                storeChunkMsg.getData().writeTo(fos);
                fos.close();

                addToChunkList(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), chunkFilePath);
            }
        }
    }

    private void addToChunkList(String fileName, int sequenceNo, Path chunkFilePath) throws IOException {
        Chunk chunk = new Chunk(fileName, sequenceNo, Files.size(chunkFilePath), chunkFilePath);
        if (chunks.get(fileName) == null) {
            chunks.put(fileName, new TreeSet<Chunk>());
        }
        chunks.get(fileName).add(chunk);
    }

    private static class HeartbeatRunnable implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
        private final ComponentAddress storageNodeAddr;
        private final ComponentAddress controllerAddr;
        private final Map<String, SortedSet<Chunk>> chunks;

        public HeartbeatRunnable(
                ComponentAddress storageNodeAddr,
                ComponentAddress controllerAddr,
                Map<String, SortedSet<Chunk>> chunks) {
            this.storageNodeAddr = storageNodeAddr;
            this.controllerAddr = controllerAddr;
            this.chunks = chunks;
        }

        @Override
        public void run() {
            try {
                Socket socket = controllerAddr.getSocket();
                int heartbeatPeriod = DFSProperties.getInstance().getHeartbeatPeriod();

                while (true) {
                    Messages.Heartbeat heartbeatMsg = Messages.Heartbeat.newBuilder()
                            .setStorageNodeHost(storageNodeAddr.getHost())
                            .setStorageNodePort(storageNodeAddr.getPort())
                            .addAllFileChunks(getFileChunks())
                            .build();
                    Messages.MessageWrapper msgWrapper =
                            Messages.MessageWrapper.newBuilder()
                                    .setHeartbeatMsg(heartbeatMsg)
                                    .build();
                    logger.trace("Sending heartbeat to controller " + controllerAddr);
                    msgWrapper.writeDelimitedTo(socket.getOutputStream());

                    Thread.sleep(heartbeatPeriod);
                }
            } catch (IOException e) {
                logger.error("Could not create socket for heartbeat", e);
            } catch (InterruptedException e) {
                logger.warn("Couldn't sleep properly", e);
            }
        }

        private Collection<Messages.Heartbeat.FileChunks> getFileChunks() {
            Set<Messages.Heartbeat.FileChunks> result = new HashSet<>();
            for (Map.Entry<String, SortedSet<Chunk>> entry : chunks.entrySet()) {
                String filename = entry.getKey();
                ArrayList<Integer> sequenceNos = new ArrayList<>(entry.getValue().size());

                for (Chunk chunk : entry.getValue()) {
                    sequenceNos.add(chunk.getSequenceNo());
                }

                Messages.Heartbeat.FileChunks fileChunksMsg = Messages.Heartbeat.FileChunks.newBuilder()
                        .setFilename(filename)
                        .addAllSequenceNos(sequenceNos)
                        .build();
                result.add(fileChunksMsg);
            }
            return result;
        }
    }

}
