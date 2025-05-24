package com.github.zvreifnitz.processor.impl.utils;

import java.util.concurrent.CompletableFuture;

public final class ProcessorFuture<T> extends CompletableFuture<T> {
    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }
}
