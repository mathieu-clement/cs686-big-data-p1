package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;

public class MessageSender implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private final StorageNodeAddressService storageNodeAddressService;
    private final Map<ComponentAddress, MessageFifoQueue> messageQueues;
    private Socket mSocket;

    public MessageSender(StorageNodeAddressService storageNodeAddressService, Map<ComponentAddress, MessageFifoQueue> messageQueues) {
        this.storageNodeAddressService = storageNodeAddressService;
        this.messageQueues = messageQueues;
    }

    @Override
    public void run() {
        ComponentAddress storageNode = storageNodeAddressService.getStorageNodeAddress();
        MessageFifoQueue messageQueue = messageQueues.get(storageNode);
        try {
            Socket socket = getSocket(storageNode);
            while (!socket.isClosed()) {
                try {
                    Messages.MessageWrapper msg = messageQueue.next();
                    logger.trace("Sending message to " + socket.getRemoteSocketAddress() + ": " + msg);
                    msg.writeDelimitedTo(socket.getOutputStream());
                } catch (InterruptedException e) {
                    logger.error("Could not get next message from queue", e);
                } catch (IOException e) {
                    logger.error("Could not send message", e);
                    try {
                        socket.close();
                    } catch (IOException ioe) {
                        logger.error("Could not close mSocket", ioe);
                    } // try close mSocket
                } // try message send
            } // while
        } catch (IOException e) {
            logger.error("Could not create mSocket to " + storageNode);
        } // try get mSocket
    } // end of method run()

    private Socket getSocket(ComponentAddress storageNodeAddress) throws IOException {
        if (mSocket == null || mSocket.isClosed()) {
            mSocket = storageNodeAddress.getSocket();
        }
        return mSocket;
    }
}
