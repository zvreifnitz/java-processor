package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.OrderedProcessor;
import com.github.zvreifnitz.processor.OrderedProcessorWorker;
import com.github.zvreifnitz.processor.ProcessorWorker;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class BasicOrderedProcessorBuilder {

    private BasicOrderedProcessorBuilder() {
    }

    static <P, V> WorkerSetter<P, V> newBuilder() {
        return new BuilderImpl<>();
    }

    public interface Builder<P, V> {
        OrderedProcessor<P, V> build();
    }

    public interface WorkerSetter<P, V> {
        ExtractorSetter<P, V> setWorker(final Consumer<V> worker);

        ExtractorSetter<P, V> setWorker(final BiConsumer<P, V> worker);

        ExtractorSetter<P, V> setWorker(final ProcessorWorker<V> worker);

        ExtractorSetter<P, V> setWorker(final OrderedProcessorWorker<P, V> worker);
    }

    public interface ExtractorSetter<P, V> {
        ExecutorSetter<P, V> setExtractor(final Function<V, P> extractor);

        ExecutorSetter<P, V> useDefaultExtractor();
    }

    public interface ExecutorSetter<P, V> {
        CloseSetter<P, V> setExecutor(final Executor executor);

        CloseSetter<P, V> useDefaultExecutor();
    }

    public interface CloseSetter<P, V> {
        Builder<P, V> setOnclose(final Runnable onClose);

        Builder<P, V> useDefaultOnClose();
    }

    private static final class BuilderImpl<P, V>
            implements WorkerSetter<P, V>, ExtractorSetter<P, V>, ExecutorSetter<P, V>,
            CloseSetter<P, V>, Builder<P, V> {

        private OrderedProcessorWorker<P, V> worker;
        private Function<V, P> extractor;
        private Executor executor;
        private Runnable onClose;

        @Override
        public Builder<P, V> setOnclose(final Runnable onClose) {
            this.onClose = requireNonNull(onClose);
            return this;
        }

        @Override
        public Builder<P, V> useDefaultOnClose() {
            this.onClose = null;
            return this;
        }

        @Override
        public CloseSetter<P, V> setExecutor(final Executor executor) {
            this.executor = requireNonNull(executor);
            return this;
        }

        @Override
        public CloseSetter<P, V> useDefaultExecutor() {
            this.executor = null;
            return this;
        }

        @Override
        public ExecutorSetter<P, V> setExtractor(final Function<V, P> extractor) {
            this.extractor = requireNonNull(extractor);
            return this;
        }

        @Override
        public ExecutorSetter<P, V> useDefaultExtractor() {
            this.extractor = null;
            return this;
        }

        @Override
        public OrderedProcessor<P, V> build() {
            return new BasicOrderedProcessor<>(worker, extractor, executor, onClose);
        }

        @Override
        public ExtractorSetter<P, V> setWorker(final Consumer<V> worker) {
            this.worker = new ConsumerAdapter<>(requireNonNull(worker));
            return this;
        }

        @Override
        public ExtractorSetter<P, V> setWorker(final BiConsumer<P, V> worker) {
            this.worker = new BiConsumerAdapter<>(requireNonNull(worker));
            return this;
        }

        @Override
        public ExtractorSetter<P, V> setWorker(final ProcessorWorker<V> worker) {
            this.worker = new ProcessorWorkerAdapter<>(requireNonNull(worker));
            return this;
        }

        @Override
        public ExtractorSetter<P, V> setWorker(final OrderedProcessorWorker<P, V> worker) {
            this.worker = requireNonNull(worker);
            return this;
        }
    }

    private record ConsumerAdapter<P, V>(Consumer<V> consumer) implements OrderedProcessorWorker<P, V> {
        @Override
        public void process(final P partition, final V value, final Iterable<V> remaining) {
            this.consumer.accept(value);
        }
    }

    private record BiConsumerAdapter<P, V>(BiConsumer<P, V> biConsumer) implements OrderedProcessorWorker<P, V> {
        @Override
        public void process(final P partition, final V value, final Iterable<V> remaining) {
            this.biConsumer.accept(partition, value);
        }
    }

    private record ProcessorWorkerAdapter<P, V>(ProcessorWorker<V> worker) implements OrderedProcessorWorker<P, V> {
        @Override
        public void process(final P partition, final V value, final Iterable<V> remaining) {
            this.worker.process(value);
        }
    }
}
