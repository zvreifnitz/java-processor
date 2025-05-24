package com.github.zvreifnitz.processor;

import com.github.zvreifnitz.processor.impl.BasicOrderedProcessor;
import com.github.zvreifnitz.processor.impl.BasicOrderedProcessorBuilder;

import java.util.concurrent.CompletableFuture;

public interface OrderedProcessor<P, V> extends Processor<V> {

    static <P, V> BasicOrderedProcessorBuilder.WorkerSetter<P, V> defaultBuilder(
            @SuppressWarnings("unused") final Class<P> partitionKeyClass,
            @SuppressWarnings("unused") final Class<V> valueClass) {
        return BasicOrderedProcessor.newBuilder();
    }

    boolean enqueue(final P partition, final V value);

    CompletableFuture<V> submit(final P partition, final V value);
}
