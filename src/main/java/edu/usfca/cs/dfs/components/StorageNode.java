package edu.usfca.cs.dfs.components;

import edu.usfca.cs.dfs.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
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

    public StorageNode(int port) {
        this.port = port;
    }

    public static void main(String[] args) 
    throws Exception {
        if (args.length != 3) {
            System.err.println("This program requires 3 arguments: storage-node-listening-port controller-address controller-listening-port");
            System.exit(1);
        }
        String hostname = getHostname();
        int port = Integer.parseInt(args[0]);
        logger.debug("Starting storage node on " + hostname + " port " + port + "...");
        new StorageNode(port).start();
    }

    public void start()
    throws Exception {
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
                logger.debug("Storing to file " + chunkFilePath);
                FileOutputStream fos = new FileOutputStream(chunkFilePath.toFile());

                storeChunkMsg.getData().writeTo(fos);
                fos.close();

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
