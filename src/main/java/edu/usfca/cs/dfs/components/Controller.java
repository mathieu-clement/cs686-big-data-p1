package edu.usfca.cs.dfs.components;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    private final int port;

    private Set<ComponentAddress> onlineStorageNodes = new HashSet<>();

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
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new ReceiveHeartbeatRunnable(onlineStorageNodes, socket)).start();
        }
    }

    private static class ReceiveHeartbeatRunnable implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(ReceiveHeartbeatRunnable.class);
        private final Set<ComponentAddress> onlineStorageNodes;
        private final Socket socket;

        public ReceiveHeartbeatRunnable(Set<ComponentAddress> onlineStorageNodes, Socket socket) {
            this.onlineStorageNodes = onlineStorageNodes;
            this.socket = socket;
        }

        @Override
        public void run() {
            while (!socket.isClosed()) {
                try {
                    Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());

                    if (msgWrapper.hasHeartbeatMsg()) {
                        ComponentAddress storageNodeAddress = new ComponentAddress(
                                msgWrapper.getHeartbeatMsg().getStorageNodeHost(),
                                msgWrapper.getHeartbeatMsg().getStorageNodePort());
                        logger.trace("Received heartbeat from " + storageNodeAddress);
                        onlineStorageNodes.add(storageNodeAddress);
                    }
                } catch (IOException e) {
                    logger.error("Error reading from heartbeat socket", e);
                }
            }
        }
    }

}
