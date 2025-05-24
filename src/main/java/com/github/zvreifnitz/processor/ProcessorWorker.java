package com.github.zvreifnitz.processor;

@FunctionalInterface
public interface ProcessorWorker<V> {
    void process(final V value);
}
