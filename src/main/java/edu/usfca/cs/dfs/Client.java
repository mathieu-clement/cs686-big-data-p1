package edu.usfca.cs.dfs;

import java.net.Socket;

import com.google.protobuf.ByteString;


public class Client {
    public static void main(String[] args)
    throws Exception {
        Socket sock = new Socket("localhost", 9999);

        ByteString data = ByteString.copyFromUtf8("Hello World!");

        StorageMessages.StoreChunk storeChunkMsg
            = StorageMessages.StoreChunk.newBuilder()
                .setFileName("my_file.txt")
                .setChunkId(3)
                .setData(data)
                .build();

        StorageMessages.StorageMessageWrapper msgWrapper =
            StorageMessages.StorageMessageWrapper.newBuilder()
                .setStoreChunkMsg(storeChunkMsg)
                .build();

        msgWrapper.writeDelimitedTo(sock.getOutputStream());

        sock.close();
    }
}
