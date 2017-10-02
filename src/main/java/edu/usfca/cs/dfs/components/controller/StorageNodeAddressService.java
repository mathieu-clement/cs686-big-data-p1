package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class StorageNodeAddressService {
    private static final Logger logger = LoggerFactory.getLogger(StorageNodeAddressService.class);
    private ComponentAddress storageNodeAddress;
    private Semaphore sema = new Semaphore(0);

    public StorageNodeAddressService() {
    }

    public ComponentAddress getStorageNodeAddress() {
        while (storageNodeAddress == null) {
            try {
                sema.acquire();
            } catch (InterruptedException e) {
                logger.error("Could not acquire semaphore for storage", e);
            }
        }
        return storageNodeAddress;
    }

    public void setStorageNodeAddress(ComponentAddress storageNodeAddress) {
        if (this.storageNodeAddress == null) {
            this.storageNodeAddress = storageNodeAddress;
            sema.release();
        } else {
            this.storageNodeAddress = storageNodeAddress;
        }
    }
}
