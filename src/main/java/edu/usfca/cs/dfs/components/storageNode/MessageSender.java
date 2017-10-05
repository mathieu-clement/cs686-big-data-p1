package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class MessageSender implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);
    private final Socket socket;
    private final BlockingQueue<Messages.MessageWrapper> messageQueue;

    public MessageSender(Socket socket, BlockingQueue<Messages.MessageWrapper> messageQueue) {
        this.socket = socket;
        this.messageQueue = messageQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Messages.MessageWrapper msg = messageQueue.take();
                msg.writeDelimitedTo(socket.getOutputStream());
            } catch (InterruptedException e) {
                logger.error("Couldn't get message from queue", e);
            } catch (IOException e) {
                logger.error("Couldn't write to socket", e);
            }
        }
    }
}
