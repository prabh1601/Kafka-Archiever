package com.prabh.Utils;

import java.util.concurrent.ArrayBlockingQueue;

public class LimitedQueue<E> extends ArrayBlockingQueue<E> {
    public LimitedQueue(int maxSize) {
        super(maxSize);
    }

    @Override
    public boolean offer(E e) {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            put(e);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
