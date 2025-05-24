package com.github.zvreifnitz.processor.impl.base;

import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.impl.utils.ExecutorUtils;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

public abstract class ExecutorProcessor<V> extends ConsumerProcessor<V>
        implements Processor<V>, Consumer<V>, AutoCloseable {

    private final Executor executor;
    private final boolean closeExecutor;
    private final boolean recursionSupported;
    private final Runnable afterClose;

    private volatile boolean stopped = false;

    protected ExecutorProcessor(final Executor executor, final Runnable afterClose) {
        this.executor = executor != null ? executor : ExecutorUtils.defaultWorkStealingPool();
        this.closeExecutor = false;
        this.recursionSupported = Boolean.FALSE.equals(ExecutorUtils.canStackOverflow(this.executor));
        this.afterClose = afterClose;
    }

    @Override
    public final boolean isClosed() {
        return this.stopped;
    }

    @Override
    public final void close() {
        this.stopped = true;
        this.doClose();
        this.closeExecutor();
        this.runAfterClose();
    }

    protected final boolean doExecute(final Runnable runnable) {
        try {
            this.executor.execute(runnable);
            return true;
        } catch (final RejectedExecutionException ignored) {
            return false;
        }
    }

    protected final boolean isRecursionSupported() {
        return this.recursionSupported;
    }

    protected void doClose() {
    }

    private void closeExecutor() {
        try {
            if (this.closeExecutor && this.executor instanceof AutoCloseable closeable) {
                closeable.close();
            }
        } catch (final Exception ignored) {
        }
    }

    private void runAfterClose() {
        try {
            if (this.afterClose != null) {
                this.afterClose.run();
            }
        } catch (final Exception ignored) {
        }
    }
}
