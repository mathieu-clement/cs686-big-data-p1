package edu.usfca.cs.dfs.components;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args)
    throws Exception {
        if (args.length != 2) {
            System.err.println("Two arguments required: controller-address controller-listening-port");
            System.exit(1);
        }

        String controllerAddr = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        String storageNodeAddr = "localhost";
        int storageNodePort = 9998;

        logger.debug("Connecting to storage node " + storageNodeAddr + ":" + storageNodePort);
        Socket sock = new Socket(storageNodeAddr, storageNodePort);

        logger.debug("Sending file 'my_file.txt' with data 'Hello World' to storage node " + storageNodeAddr);
        ByteString data = ByteString.copyFromUtf8("Hello World!");

        Messages.StoreChunk storeChunkMsg
                = Messages.StoreChunk.newBuilder()
                .setFileName("my_file.txt")
                .setChunkId(3)
                .setData(data)
                .build();

        Messages.MessageWrapper msgWrapper =
                Messages.MessageWrapper.newBuilder()
                .setStoreChunkMsg(storeChunkMsg)
                .build();

        msgWrapper.writeDelimitedTo(sock.getOutputStream());

        logger.debug("Close connection to storage node " + storageNodeAddr);
        sock.close();
    }
}
