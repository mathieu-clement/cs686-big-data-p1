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
    private final Socket socket;

    public MessageSender(StorageNodeAddressService storageNodeAddressService, Map<ComponentAddress, MessageFifoQueue> messageQueues, Socket socket) {
        this.storageNodeAddressService = storageNodeAddressService;
        this.messageQueues = messageQueues;
        this.socket = socket;
    }

    @Override
    public void run() {
        MessageFifoQueue messageQueue = messageQueues.get(storageNodeAddressService.getStorageNodeAddress());
        while (!socket.isClosed()) {
            try {
                Messages.MessageWrapper msg = messageQueue.next();
                logger.trace("Sending message to " + socket.getRemoteSocketAddress() + ": " + msg);
                msg.writeDelimitedTo(socket.getOutputStream());
            } catch (InterruptedException e) {
                logger.error("Could not get next message from queue", e);
            } catch (IOException e) {
                logger.error("Could not send message", e);
            }
        }
    }
}
