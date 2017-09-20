package edu.usfca.cs.dfs.components;

import edu.usfca.cs.dfs.messages.Messages;
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
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageNode {

    private final static Logger logger = LoggerFactory.getLogger(StorageNode.class);

    private ServerSocket srvSocket;

    private final int port;
    private final ComponentAddress controllerAddr;

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

    public void start()
    throws Exception {
        new Thread(new HeartbeatRunnable(new ComponentAddress(getHostname(), port), controllerAddr)).start();

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

            }
        }
    }

    private static class HeartbeatRunnable implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
        private final ComponentAddress storageNodeAddr;
        private final ComponentAddress controllerAddr;

        public HeartbeatRunnable(ComponentAddress storageNodeAddr, ComponentAddress controllerAddr) {
            this.storageNodeAddr = storageNodeAddr;
            this.controllerAddr = controllerAddr;
        }

        @Override
        public void run() {
            try {
                Socket socket = controllerAddr.getSocket();

                while (true) {
                    Messages.Heartbeat heartbeatMsg = Messages.Heartbeat.newBuilder()
                            .setStorageNodeHost(storageNodeAddr.getHost())
                            .setStorageNodePort(storageNodeAddr.getPort())
                            .build();
                    Messages.MessageWrapper msgWrapper =
                            Messages.MessageWrapper.newBuilder()
                                    .setHeartbeatMsg(heartbeatMsg)
                                    .build();
                    logger.trace("Sending heartbeat to controller " + controllerAddr);
                    msgWrapper.writeDelimitedTo(socket.getOutputStream());

                    Thread.sleep(5000);
                }
            } catch (IOException e) {
                logger.error("Could not create socket for heartbeat", e);
            } catch (InterruptedException e) {
                logger.warn("Couldn't sleep properly", e);
            }
        }
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

}
