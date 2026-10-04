package com.github.zvreifnitz.processor.impl.utils;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

public interface TaskTracker {
    static TaskTracker unbounded() {
        return new CounterTaskTracker();
    }

    static TaskTracker bounded(final int size) {
        return new SemaphoreTaskTracker(size);
    }

    boolean acquire(final int count);

    void release(final int count);

    int count();

    void awaitAll();
}


class CounterTaskTracker implements TaskTracker {
    private final LongAdder counter = new LongAdder();

    @Override
    public final boolean acquire(final int count) {
        this.counter.add(count);
        return true;
    }

    @Override
    public final void release(final int count) {
        this.counter.add(-count);
    }

    @Override
    public final int count() {
        return this.counter.intValue();
    }

    @Override
    public final void awaitAll() {
        while (this.count() != 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }
}

class SemaphoreTaskTracker implements TaskTracker {
    private final Semaphore semaphore;
    private final int size;

    public SemaphoreTaskTracker(final int size) {
        this.size = size;
        this.semaphore = new Semaphore(size);
    }

    @Override
    public final boolean acquire(final int count) {
        return this.semaphore.tryAcquire(count);
    }

    @Override
    public final void release(final int count) {
        this.semaphore.release(count);
    }

    @Override
    public final int count() {
        return this.size - this.semaphore.availablePermits();
    }

    @Override
    public final void awaitAll() {
        boolean inProgress = true;
        while (inProgress) {
            try {
                this.semaphore.acquire(this.size);
                inProgress = false;
            } catch (final InterruptedException ignored) {
            }
        }
    }
}