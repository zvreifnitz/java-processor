package com.github.zvreifnitz.processor;

import com.github.zvreifnitz.processor.impl.BasicProcessor;
import com.github.zvreifnitz.processor.impl.BasicProcessorBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface Processor<V> extends Consumer<V>, AutoCloseable {

    static <V> BasicProcessorBuilder.WorkerSetter<V> defaultBuilder(
            @SuppressWarnings("unused") final Class<V> v) {
        return BasicProcessor.newBuilder();
    }

    boolean enqueue(final V value);

    CompletableFuture<V> submit(final V value);

    int count();

    boolean isOpen();

    boolean isClosed();

    void close();
}
