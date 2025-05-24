package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.ProcessorWorker;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class BasicProcessorBuilder {

    private BasicProcessorBuilder() {
    }

    static <V> WorkerSetter<V> newBuilder() {
        return new BuilderImpl<>();
    }

    public interface Builder<V> {
        Processor<V> build();
    }

    public interface WorkerSetter<V> {
        ExecutorSetter<V> setWorker(final Consumer<V> worker);

        ExecutorSetter<V> setWorker(final ProcessorWorker<V> worker);
    }

    public interface ExecutorSetter<V> {
        CloseSetter<V> setExecutor(final Executor executor);

        CloseSetter<V> useDefaultExecutor();
    }

    public interface CloseSetter<V> {
        Builder<V> setOnclose(final Runnable onClose);

        Builder<V> useDefaultOnClose();
    }

    private static final class BuilderImpl<V>
            implements WorkerSetter<V>, ExecutorSetter<V>, CloseSetter<V>, Builder<V> {

        private ProcessorWorker<V> worker;
        private Executor executor;
        private Runnable onClose;

        @Override
        public CloseSetter<V> setExecutor(final Executor executor) {
            this.executor = requireNonNull(executor);
            return this;
        }

        @Override
        public CloseSetter<V> useDefaultExecutor() {
            this.executor = null;
            return this;
        }

        @Override
        public Builder<V> setOnclose(final Runnable onClose) {
            this.onClose = requireNonNull(onClose);
            return this;
        }

        @Override
        public Builder<V> useDefaultOnClose() {
            this.onClose = null;
            return this;
        }

        @Override
        public Processor<V> build() {
            return new BasicProcessor<>(worker, executor, onClose);
        }

        @Override
        public ExecutorSetter<V> setWorker(final Consumer<V> worker) {
            this.worker = new ConsumerAdapter<>(requireNonNull(worker));
            return this;
        }

        @Override
        public ExecutorSetter<V> setWorker(final ProcessorWorker<V> worker) {
            this.worker = requireNonNull(worker);
            return this;
        }
    }

    private record ConsumerAdapter<V>(Consumer<V> consumer) implements ProcessorWorker<V> {
        @Override
        public void process(final V value) {
            this.consumer.accept(value);
        }
    }
}
