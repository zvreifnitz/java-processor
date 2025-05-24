package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.ProcessorWorker;
import com.github.zvreifnitz.processor.impl.base.ExecutorProcessor;
import com.github.zvreifnitz.processor.impl.utils.ProcessorFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class BasicProcessor<V> extends ExecutorProcessor<V>
        implements Processor<V>, Consumer<V>, AutoCloseable {

    private final ProcessorWorker<V> worker;
    private final LongAdder counter;

    public BasicProcessor(final ProcessorWorker<V> worker, final Executor executor, final Runnable afterClose) {
        super(executor, afterClose);
        this.worker = requireNonNull(worker);
        this.counter = new LongAdder();
    }

    public static <V> BasicProcessorBuilder.WorkerSetter<V> newBuilder() {
        return BasicProcessorBuilder.newBuilder();
    }

    @Override
    public boolean enqueue(final V value) {
        return this.enqueueTask(new EnqueueTask<>(value, this));
    }

    @Override
    public CompletableFuture<V> submit(final V value) {
        final ProcessorFuture<V> future = new ProcessorFuture<>();
        if (!this.enqueueTask(new SubmitTask<>(value, future, this))) {
            future.completeExceptionally(new RejectedExecutionException("Submitting value failed"));
        }
        return future;
    }

    @Override
    public int count() {
        return this.counter.intValue();
    }

    @Override
    protected void doClose() {
        while (this.count() != 0) {
            Thread.yield();
        }
        super.doClose();
    }

    private boolean enqueueTask(final Runnable task) {
        this.counter.increment();
        if (this.isOpen() && this.doExecute(task)) {
            return true;
        }
        this.counter.decrement();
        return false;
    }

    private record EnqueueTask<V>(V value, BasicProcessor<V> parent) implements Runnable {
        @Override
        public void run() {
            try {
                this.parent.worker.process(this.value);
            } finally {
                this.parent.counter.decrement();
            }
        }
    }

    private record SubmitTask<V>(V value, ProcessorFuture<V> future, BasicProcessor<V> parent) implements Runnable {
        @Override
        public void run() {
            try {
                this.parent.worker.process(this.value);
                this.future.complete(this.value);
            } catch (final Exception e) {
                this.future.completeExceptionally(e);
            } finally {
                this.parent.counter.decrement();
            }
        }
    }
}
