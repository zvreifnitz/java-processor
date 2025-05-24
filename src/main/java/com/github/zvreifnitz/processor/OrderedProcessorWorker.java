package com.github.zvreifnitz.processor;

@FunctionalInterface
public interface OrderedProcessorWorker<P, V> {
    void process(final P partition, final V value, final Iterable<V> remaining);
}
