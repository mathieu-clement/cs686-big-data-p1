package edu.usfca.cs.dfs.components;

import edu.usfca.cs.dfs.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

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
                        + storeChunkMsg.getFileName() + " received from " +
                        socket.getRemoteSocketAddress().toString());
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
