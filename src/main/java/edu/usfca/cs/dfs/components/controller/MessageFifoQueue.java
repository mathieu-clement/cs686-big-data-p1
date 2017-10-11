package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.messages.Messages;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageFifoQueue {
    private List<Messages.MessageWrapper> messages = new LinkedList<>();
    private Lock listLock = new ReentrantLock();

    private Semaphore waitForNextMessageSema = new Semaphore(0);

    public void queue(Messages.MessageWrapper msg) {
        listLock.lock();
        messages.add(msg);
        listLock.unlock();
        waitForNextMessageSema.release();
    }

    /**
     * Get next, waiting until available
     *
     * @return oldest message in queue
     * @throws InterruptedException in case of synchronization issues
     */
    public Messages.MessageWrapper next() throws InterruptedException {
        do {
            waitForNextMessageSema.acquire();
        } while (messages.size() == 0);

        listLock.lock();
        try {
            Messages.MessageWrapper msg = messages.get(0);
            messages.remove(0);
            return msg;
        } finally {
            listLock.unlock();
        }
    }

}
