package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    private final int port;

    private final Set<ComponentAddress> onlineStorageNodes = new HashSet<>();

    private final FileTable fileTable = new FileTable();

    private final Map<ComponentAddress, MessageFifoQueue> messageQueues = new HashMap<>();

    public Controller(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: Controller port");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);

        logger.info("Starting controller...");

        new Controller(port).start();
    }

    public void start() throws Exception {
        ServerSocket serverSocket = new ServerSocket(port);

        new Thread(new ChunkReplicationRunnable(onlineStorageNodes, messageQueues, fileTable)).start();

        while (true) {
            Socket socket = serverSocket.accept();
            StorageNodeAddressService storageNodeAddressService = new StorageNodeAddressService();
            new Thread(new MessageProcessor(storageNodeAddressService, onlineStorageNodes, messageQueues, fileTable, socket)).start();

            // Though it will work fine without it, sleeping for a while will avoid the message sender
            // to run into a race condition where the message queue doesn't exist yet because we have not
            // parsed the heartbeat to figure out the hostname and port of the storage node.
            // This avoids an error to be displayed every time a storage node gets online.
            Thread.sleep(1000);
            new Thread(new MessageSender(storageNodeAddressService, messageQueues, socket)).start();
        }
    }

}
