package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.ProcessorWorker;
import com.github.zvreifnitz.processor.impl.base.ExecutorProcessor;
import com.github.zvreifnitz.processor.impl.utils.ProcessorFuture;
import com.github.zvreifnitz.processor.impl.utils.TaskTracker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class BasicProcessor<V> extends ExecutorProcessor<V>
        implements Processor<V>, Consumer<V>, AutoCloseable {

    private final ProcessorWorker<V> worker;
    private final TaskTracker tracker;

    public BasicProcessor(final ProcessorWorker<V> worker, final TaskTracker tracker, final Executor executor, final Runnable afterClose) {
        super(executor, afterClose);
        this.worker = requireNonNull(worker);
        this.tracker = requireNonNull(tracker);
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
        return this.tracker.count();
    }

    @Override
    protected void doClose() {
        this.tracker.awaitAll();
        super.doClose();
    }

    private boolean enqueueTask(final Runnable task) {
        if (this.isOpen() && this.tracker.acquire(1)) {
            if (this.isOpen() && this.doExecute(task)) {
                return true;
            }
            this.tracker.release(1);
        }
        return false;
    }

    private record EnqueueTask<V>(V value, BasicProcessor<V> parent) implements Runnable {
        @Override
        public void run() {
            try {
                this.parent.worker.process(this.value);
            } finally {
                this.parent.tracker.release(1);
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
                this.parent.tracker.release(1);
            }
        }
    }
}
