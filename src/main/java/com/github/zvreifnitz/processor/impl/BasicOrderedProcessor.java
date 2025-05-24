package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.OrderedProcessor;
import com.github.zvreifnitz.processor.OrderedProcessorWorker;
import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.impl.base.ExecutorProcessor;
import com.github.zvreifnitz.processor.impl.utils.ProcessorFuture;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class BasicOrderedProcessor<P, V> extends ExecutorProcessor<V>
        implements OrderedProcessor<P, V>, Processor<V>, Consumer<V>, AutoCloseable {

    private final OrderedProcessorWorker<P, V> worker;
    private final Function<V, PartitionKey<P>> extractor;
    private final ConcurrentMap<PartitionKey<P>, Partition<V>> partitions;
    private final LongAdder counter;

    public BasicOrderedProcessor(
            final OrderedProcessorWorker<P, V> worker,
            final Function<V, P> extractor,
            final Executor executor,
            final Runnable afterClose) {
        super(executor, afterClose);
        this.worker = requireNonNull(worker);
        this.extractor = extractor == null ? new EqualsExtractor<>() : new FunctionExtractor<>(extractor);
        this.partitions = new ConcurrentHashMap<>();
        this.counter = new LongAdder();
    }

    public static <P, V> BasicOrderedProcessorBuilder.WorkerSetter<P, V> newBuilder() {
        return BasicOrderedProcessorBuilder.newBuilder();
    }

    @Override
    public boolean enqueue(final V value) {
        return this.enqueue(this.extractor.apply(value), new Data<>(value, null));
    }

    @Override
    public boolean enqueue(final P partition, final V value) {
        return this.enqueue(new ValueKey<>(partition), new Data<>(value, null));
    }

    @Override
    public CompletableFuture<V> submit(final V value) {
        return this.submit(this.extractor.apply(value), new Data<>(value, new ProcessorFuture<>()));
    }

    @Override
    public CompletableFuture<V> submit(final P partition, final V value) {
        return this.submit(new ValueKey<>(partition), new Data<>(value, new ProcessorFuture<>()));
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

    private ProcessorFuture<V> submit(final PartitionKey<P> partitionKey, final Data<V> data) {
        if (!this.enqueue(partitionKey, data)) {
            data.getFuture().completeExceptionally(new RejectedExecutionException("Submitting value failed"));
        }
        return data.getFuture();
    }

    private boolean enqueue(final PartitionKey<P> partitionKey, final Data<V> data) {
        this.counter.increment();
        final Cons<V> queue = new Cons<>(data);
        while (this.isOpen()) {
            final Partition<V> partition = this.getPartition(partitionKey, queue);
            if (partition == null) {
                return this.enqueueTask(new Task<>(this, partitionKey, queue, data));
            }
            if (partition.append(queue)) {
                return true;
            }
        }
        this.counter.decrement();
        return false;
    }

    private Partition<V> getPartition(final PartitionKey<P> partitionKey, final Cons<V> queue) {
        final Partition<V> existing = this.partitions.get(partitionKey);
        return existing != null ? existing : this.partitions.putIfAbsent(partitionKey, new Partition<>(queue));
    }

    private boolean enqueueTask(final Task<P, V> task) {
        return this.doExecute(task);
    }

    private record EqualsExtractor<P, V>() implements Function<V, PartitionKey<P>> {
        @Override
        public PartitionKey<P> apply(final V value) {
            return new EqualsKey<>(value);
        }
    }

    private record FunctionExtractor<P, V>(Function<V, P> extractor) implements Function<V, PartitionKey<P>> {
        @Override
        public PartitionKey<P> apply(final V value) {
            return new ValueKey<>(this.extractor.apply(value));
        }
    }

    private static final class Task<P, V> implements Runnable {

        private final BasicOrderedProcessor<P, V> parent;
        private final PartitionKey<P> key;
        private Cons<V> queue;
        private Data<V> data;

        public Task(
                final BasicOrderedProcessor<P, V> parent, final PartitionKey<P> key,
                final Cons<V> queue, final Data<V> data) {
            this.parent = parent;
            this.key = key;
            this.queue = queue;
            this.data = data;
        }

        @Override
        public void run() {
            if (this.parent.isRecursionSupported()) {
                final Cons<V> remainingQueue = this.processItem(this.queue, this.data);
                if (remainingQueue != null) {
                    this.queue = remainingQueue;
                    this.data = remainingQueue.getData();
                    this.parent.enqueueTask(this);
                }
            } else {
                final Cons<V> q = this.queue;
                final Data<V> d = this.data;
                this.queue = null;
                this.data = null;
                this.processAllItems(q, d);
            }
        }

        private void processAllItems(final Cons<V> queue, final Data<V> data) {
            Cons<V> remainingQueue = this.processItem(queue, data);
            while (remainingQueue != null) {
                remainingQueue = this.processItem(remainingQueue, remainingQueue.getData());
            }
        }

        private Cons<V> processItem(final Cons<V> queue, final Data<V> data) {
            this.processData(queue, data);
            this.parent.counter.add(data.getDoneCount());
            return this.getRemainingQueueLoop(queue);
        }

        private void processData(final Iterable<V> queue, final Data<V> data) {
            try {
                final P key = this.key.getPartition();
                this.parent.worker.process(key, data.getValue(), queue);
                this.complete(data);
            } catch (final Exception e) {
                this.completeExceptionally(data, e);
            }
        }

        private void complete(final Fields<V> data) {
            final ArrayList<Fields<V>> submits = data.getDoneSubmits();
            if (submits != null) {
                for (final var d : submits) {
                    d.getFuture().complete(d.getValue());
                }
            }
        }

        private void completeExceptionally(final Fields<V> data, final Exception e) {
            final ArrayList<Fields<V>> submits = data.getDoneSubmits();
            if (submits != null) {
                for (final var d : submits) {
                    d.getFuture().completeExceptionally(e);
                }
            }
        }

        private Cons<V> getRemainingQueueLoop(final Cons<V> queue) {
            Cons<V> remainingQueue = queue;
            do {
                remainingQueue = getRemainingQueue(remainingQueue);
            } while (remainingQueue != null && remainingQueue.getData().isDone());
            return remainingQueue;
        }

        private Cons<V> getRemainingQueue(final Cons<V> queue) {
            final Queue<V> existingQueue = queue.getRemainingQueue();
            final Queue<V> appendedQueue = existingQueue == null ? queue.tryAppend(Nil.nil()) : existingQueue;
            if (appendedQueue instanceof Cons<V> q) {
                return q;
            }
            this.parent.partitions.remove(this.key);
            return null;
        }
    }

    private static sealed abstract class PartitionKey<P> permits ValueKey, EqualsKey {
        public abstract P getPartition();
    }

    private static final class ValueKey<P> extends PartitionKey<P> {

        private final P key;

        public ValueKey(final P key) {
            this.key = key;
        }

        @Override
        public P getPartition() {
            return this.key;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;

            final ValueKey<?> other = (ValueKey<?>) object;
            return Objects.equals(this.key, other.key);
        }

        @Override
        public int hashCode() {
            return this.key == null ? -827727347 : this.key.hashCode();
        }
    }

    private static final class EqualsKey<P, V> extends PartitionKey<P> {

        private final V value;

        public EqualsKey(final V value) {
            this.value = value;
        }

        @Override
        public P getPartition() {
            return null;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;

            final EqualsKey<?, ?> other = (EqualsKey<?, ?>) object;
            return Objects.equals(this.value, other.value);
        }

        @Override
        public int hashCode() {
            return this.value == null ? -746635477 : this.value.hashCode();
        }
    }

    private static final class Partition<V> {

        private static final VarHandle QUEUE;

        static {
            try {
                QUEUE = MethodHandles.lookup().findVarHandle(Partition.class, "queue", Cons.class);
            } catch (final ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @SuppressWarnings("unused")
        private volatile Cons<V> queue;

        public Partition(final Cons<V> queue) {
            this.setQueue(queue);
        }

        public boolean append(final Cons<V> queue) {
            final Cons<V> q = this.getQueue();
            if (q.append(queue)) {
                this.setQueue(queue);
                return true;
            }
            return false;
        }

        private Cons<V> getQueue() {
            @SuppressWarnings("unchecked") final Cons<V> queue = (Cons<V>) QUEUE.getOpaque(this);
            return queue;
        }

        private void setQueue(final Cons<V> queue) {
            QUEUE.setOpaque(this, queue);
        }
    }

    private static abstract class Fields<V> {

        private final V value;
        private final ProcessorFuture<V> future;
        private boolean done;
        private int doneCount = -1;
        private ArrayList<Fields<V>> doneSubmits;

        public Fields(final V value, final ProcessorFuture<V> future) {
            this.value = value;
            this.future = future;
            this.doneSubmits = future != null ? new ArrayList<>(List.of(this)) : null;
        }

        public V getValue() {
            return this.value;
        }

        public ProcessorFuture<V> getFuture() {
            return this.future;
        }

        public boolean isDone() {
            return this.done;
        }

        public int getDoneCount() {
            return this.doneCount;
        }

        public ArrayList<Fields<V>> getDoneSubmits() {
            return this.doneSubmits;
        }

        public void markAsDone(final Fields<V> data) {
            data.done = true;
            if (data.future != null) {
                ArrayList<Fields<V>> result = this.doneSubmits;
                if (result == null) {
                    this.doneSubmits = result = new ArrayList<>();
                }
                result.add(data);
            }
            this.doneCount--;
        }
    }

    @SuppressWarnings("unused")
    private static final class Data<V> extends Fields<V> {

        private volatile long p7;
        private volatile long p8;
        private volatile long p9;
        private volatile long p10;
        private volatile long p11;

        public Data(final V value, final ProcessorFuture<V> future) {
            super(value, future);
        }
    }

    @SuppressWarnings("unused")
    private static sealed abstract class Queue<V> permits Cons, Nil {
    }

    private static final class Cons<V> extends Queue<V> implements Iterable<V> {

        private static final VarHandle REMAINING_QUEUE;

        static {
            try {
                REMAINING_QUEUE = MethodHandles.lookup().findVarHandle(Cons.class, "remainingQueue", Queue.class);
            } catch (final ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final Data<V> data;
        @SuppressWarnings("unused")
        private volatile Queue<V> remainingQueue;

        public Cons(final Data<V> data) {
            this.data = data;
        }

        public Data<V> getData() {
            return this.data;
        }

        public Queue<V> getRemainingQueue() {
            @SuppressWarnings("unchecked") final Queue<V> result = (Queue<V>) REMAINING_QUEUE.getAcquire(this);
            return result;
        }

        public boolean append(final Cons<V> queue) {
            Cons<V> current = this;
            while (true) {
                final Queue<V> existingQueue = current.getRemainingQueue();
                final Queue<V> appendedQueue = existingQueue == null ? current.tryAppend(queue) : existingQueue;
                if (appendedQueue instanceof Cons<V> q) {
                    current = q;
                    continue;
                }
                return appendedQueue == null;
            }
        }

        public Queue<V> tryAppend(final Queue<V> queue) {
            @SuppressWarnings("unchecked") final Queue<V> result =
                    (Queue<V>) REMAINING_QUEUE.compareAndExchangeRelease(this, null, queue);
            return result;
        }

        @Override
        public Iterator<V> iterator() {
            return new QueueIterator<>(this);
        }
    }

    private static final class Nil<V> extends Queue<V> {

        private static final Nil<?> NIL = new Nil<>();

        public static <V> Nil<V> nil() {
            @SuppressWarnings("unchecked") final Nil<V> result = (Nil<V>) NIL;
            return result;
        }
    }

    private static final class QueueIterator<V> implements Iterator<V> {

        private final Data<V> root;
        private Cons<V> queue;
        private Data<V> data;
        private boolean removeAllowed;
        private Cons<V> cached;

        public QueueIterator(final Cons<V> queue) {
            this.queue = queue;
            this.root = this.data = queue.getData();
        }

        @Override
        public boolean hasNext() {
            return this.getNextCache() != null;
        }

        @Override
        public V next() {
            final Cons<V> remainingQueue = this.getCacheNext();
            final boolean nextFound = this.removeAllowed = remainingQueue != null;
            if (nextFound) {
                this.queue = remainingQueue;
                return (this.data = remainingQueue.getData()).getValue();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (this.removeAllowed) {
                this.removeAllowed = false;
                this.root.markAsDone(this.data);
            } else {
                throw new IllegalStateException();
            }
        }

        private Cons<V> getNextCache() {
            return this.cached = this.getRemainingQueue();
        }

        private Cons<V> getCacheNext() {
            final Cons<V> remainingQueue = this.cached;
            this.cached = null;
            return remainingQueue != null ? remainingQueue : this.getRemainingQueue();
        }

        private Cons<V> getRemainingQueue() {
            Cons<V> remainingQueue = this.queue;
            do {
                remainingQueue = remainingQueue.getRemainingQueue() instanceof Cons<V> q ? q : null;
            } while (remainingQueue != null && remainingQueue.getData().isDone());
            return remainingQueue;
        }
    }
}