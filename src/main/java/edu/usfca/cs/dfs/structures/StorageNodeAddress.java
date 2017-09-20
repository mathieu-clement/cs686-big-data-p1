package edu.usfca.cs.dfs.structures;

public class StorageNodeAddress {
    private final String host;
    private final int port;

    public StorageNodeAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + port;
    }
}
