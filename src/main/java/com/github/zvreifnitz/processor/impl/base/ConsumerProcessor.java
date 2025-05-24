package com.github.zvreifnitz.processor.impl.base;

import com.github.zvreifnitz.processor.Processor;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

public abstract class ConsumerProcessor<V> implements Processor<V>, Consumer<V>, AutoCloseable {

    protected ConsumerProcessor() {
    }

    @Override
    public final void accept(final V value) {
        if (!this.enqueue(value)) {
            throw new RejectedExecutionException();
        }
    }

    @Override
    public final boolean isOpen() {
        return !this.isClosed();
    }
}
