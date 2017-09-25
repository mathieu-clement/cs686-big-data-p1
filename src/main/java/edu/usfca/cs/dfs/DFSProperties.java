package edu.usfca.cs.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class DFSProperties {
    private static final DFSProperties INSTANCE = new DFSProperties();
    private static final Logger logger = LoggerFactory.getLogger(DFSProperties.class);
    private final Properties properties;

    private DFSProperties() {
        properties = new Properties();
        try {
            properties.load(DFSProperties.class.getClassLoader()
                    .getResourceAsStream("dfs.properties"));
        } catch (IOException e) {
            logger.error("Couldn't load properties from file", e);
        }
    }

    public static DFSProperties getInstance() {
        return INSTANCE;
    }

    public int getHeartbeatPeriod() {
        return Integer.parseInt(properties.getProperty("heartbeat-period"));
    }

    public long getChunkSize() {
        return Long.parseLong(properties.getProperty("chunk-size"));
    }

    public String getClientChunksDir() {
        return properties.getProperty("client-chunks-dir");
    }

    public String getStorageNodeChunksDir() {
        return properties.getProperty("storage-node-chunks-dir");
    }

    public int getMinReplicas() {
        return Integer.parseInt(properties.getProperty("min-replicas"));
    }
}
