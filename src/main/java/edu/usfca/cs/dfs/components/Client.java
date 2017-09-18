package edu.usfca.cs.dfs.components;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.messages.Messages;

import java.net.Socket;

public class Client {
    public static void main(String[] args)
    throws Exception {
        if (args.length != 2) {
            System.err.println("Two arguments required: controller-address controller-listening-port");
            System.exit(1);
        }

        String controllerAddr = args[0];
        int controllerPort = Integer.parseInt(args[1]);

        Socket sock = new Socket(controllerAddr, controllerPort);

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

        sock.close();
    }
}
