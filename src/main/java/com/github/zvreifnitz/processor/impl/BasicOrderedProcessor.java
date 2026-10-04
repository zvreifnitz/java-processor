package com.github.zvreifnitz.processor.impl;

import com.github.zvreifnitz.processor.OrderedProcessor;
import com.github.zvreifnitz.processor.OrderedProcessorWorker;
import com.github.zvreifnitz.processor.Processor;
import com.github.zvreifnitz.processor.impl.base.ExecutorProcessor;
import com.github.zvreifnitz.processor.impl.utils.ProcessorFuture;
import com.github.zvreifnitz.processor.impl.utils.TaskTracker;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class BasicOrderedProcessor<P, V> extends ExecutorProcessor<V>
        implements OrderedProcessor<P, V>, Processor<V>, Consumer<V>, AutoCloseable {

    private final OrderedProcessorWorker<P, V> worker;
    private final Function<V, P> extractor;
    private final PartitionQueue<P, V> partitions;
    private final TaskTracker tracker;

    public BasicOrderedProcessor(
            final OrderedProcessorWorker<P, V> worker,
            final Function<V, P> extractor,
            final TaskTracker tracker,
            final Executor executor,
            final Runnable afterClose) {
        super(executor, afterClose);
        this.worker = requireNonNull(worker);
        this.extractor = requireNonNull(extractor);
        this.tracker = requireNonNull(tracker);
        this.partitions = this.getInfo().virtualThread() ? new SyncPartitionQueue<>() : new ChmPartitionQueue<>();
    }

    public static <P, V> BasicOrderedProcessorBuilder.WorkerSetter<P, V> newBuilder() {
        return BasicOrderedProcessorBuilder.newBuilder();
    }

    @Override
    public boolean enqueue(final V value) {
        final P partitionKey = requireNonNull(this.extractor.apply(value));
        return this.enqueue(partitionKey, new Node<>(value, null));
    }

    @Override
    public boolean enqueue(final P partition, final V value) {
        final P partitionKey = requireNonNull(partition);
        return this.enqueue(partitionKey, new Node<>(value, null));
    }

    @Override
    public CompletableFuture<V> submit(final V value) {
        final P partitionKey = requireNonNull(this.extractor.apply(value));
        return this.submit(partitionKey, new Node<>(value, new ProcessorFuture<>()));
    }

    @Override
    public CompletableFuture<V> submit(final P partition, final V value) {
        final P partitionKey = requireNonNull(partition);
        return this.submit(partitionKey, new Node<>(value, new ProcessorFuture<>()));
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

    private ProcessorFuture<V> submit(final P partitionKey, final Node<V> node) {
        if (!this.enqueue(partitionKey, node)) {
            node.future.completeExceptionally(new RejectedExecutionException("Submitting value failed"));
        }
        return node.future;
    }

    private boolean enqueue(final P partitionKey, final Node<V> node) {
        if (this.isOpen() && this.tracker.acquire(1)) {
            while (this.isOpen()) {
                final Partition<V> partition = this.partitions.getPartition(partitionKey, node);
                if (partition == null) {
                    this.enqueueTask(new Task<>(this, partitionKey, node));
                    return true;
                }
                if (partition.append(node)) {
                    return true;
                }
            }
            this.tracker.release(1);
        }
        return false;
    }

    private void enqueueTask(final Task<P, V> task) {
        if (!this.doExecute(task)) {
            task.run();
        }
    }

    private static final class Task<P, V> implements Iterable<V>, Runnable {

        private final BasicOrderedProcessor<P, V> parent;
        private final P key;
        private Node<V> node;
        private Node<V> current;
        private List<Node<V>> doneNodes;

        public Task(
                final BasicOrderedProcessor<P, V> parent, final P key, final Node<V> node) {
            this.parent = parent;
            this.key = key;
            this.node = node;
        }

        @Override
        public void run() {
            if (this.parent.getInfo().recursionSafe()) {
                final Node<V> remaining = this.processNode(this.node);
                if (remaining != null) {
                    this.node = remaining;
                    this.parent.enqueueTask(this);
                }
            } else {
                final Node<V> q = this.node;
                this.node = null;
                this.processAllINodes(q);
            }
        }

        private void processAllINodes(final Node<V> node) {
            Node<V> remaining = this.processNode(node);
            while (remaining != null) {
                remaining = this.processNode(remaining);
            }
        }

        private Node<V> processNode(final Node<V> node) {
            this.execute(node);
            return this.getRemainingNodeLoop(node);
        }

        private void execute(final Node<V> node) {
            try {
                this.current = node;
                this.parent.worker.process(this.key, node.value, this);
                this.complete();
            } catch (final Exception e) {
                this.completeExceptionally(e);
            }
        }

        private Node<V> getRemainingNodeLoop(final Node<V> node) {
            Node<V> remaining = node;
            do {
                remaining = getRemainingNode(remaining);
            } while (remaining != null && remaining.done);
            return remaining;
        }

        private Node<V> getRemainingNode(final Node<V> node) {
            final Queue<V> existing = node.getRemainingQueue();
            final Queue<V> appended = existing == null ? node.tryAppend(Nil.nil()) : existing;
            if (appended instanceof Node<V> n) {
                return n;
            }
            this.parent.partitions.remove(this.key);
            return null;
        }

        @Override
        public Iterator<V> iterator() {
            if (this.doneNodes == null) {
                this.doneNodes = new ArrayList<>();
            }
            return new QueueIterator<>(this);
        }

        public void complete() {
            if (this.current.future != null) {
                this.current.future.complete(this.current.value);
            }
            int count = 1;
            if (this.doneNodes != null) {
                for (final Node<V> node : this.doneNodes) {
                    count++;
                    node.done = true;
                    if (node.future != null) {
                        node.future.complete(node.value);
                    }
                }
                this.doneNodes = null;
            }
            this.parent.tracker.release(count);
        }

        public void completeExceptionally(final Exception exception) {
            if (this.current.future != null) {
                this.current.future.completeExceptionally(exception);
            }
            int count = 1;
            if (this.doneNodes != null) {
                for (final Node<V> node : this.doneNodes) {
                    count++;
                    node.done = true;
                    if (node.future != null) {
                        node.future.completeExceptionally(exception);
                    }
                }
                this.doneNodes = null;
            }
            this.parent.tracker.release(count);
        }

        public void addToDoneNode(final Node<V> node) {
            this.doneNodes.add(node);
        }
    }

    private static final class Partition<V> {

        private static final VarHandle ROOT;

        static {
            try {
                ROOT = MethodHandles.lookup().findVarHandle(Partition.class, "root", Node.class);
            } catch (final ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @SuppressWarnings("unused")
        private volatile Node<V> root;

        public Partition(final Node<V> node) {
            this.setRoot(node);
        }

        public boolean append(final Node<V> node) {
            final Node<V> n = this.getRoot();
            if (n.append(node)) {
                this.setRoot(node);
                return true;
            }
            return false;
        }

        private Node<V> getRoot() {
            @SuppressWarnings("unchecked") final Node<V> n = (Node<V>) ROOT.getOpaque(this);
            return n;
        }

        private void setRoot(final Node<V> node) {
            ROOT.setOpaque(this, node);
        }
    }

    @SuppressWarnings("unused")
    private static sealed abstract class Queue<V> permits Node, Nil {
    }

    private static final class Node<V> extends Queue<V> {

        private static final VarHandle REMAINING_QUEUE;

        static {
            try {
                REMAINING_QUEUE = MethodHandles.lookup().findVarHandle(Node.class, "remainingQueue", Queue.class);
            } catch (final ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final V value;
        private final ProcessorFuture<V> future;
        @SuppressWarnings("unused")
        private volatile Queue<V> remainingQueue;
        private boolean done = false;

        public Node(final V value, final ProcessorFuture<V> future) {
            this.value = value;
            this.future = future;
        }

        public Queue<V> getRemainingQueue() {
            @SuppressWarnings("unchecked") final Queue<V> q = (Queue<V>) REMAINING_QUEUE.getAcquire(this);
            return q;
        }

        public boolean append(final Node<V> node) {
            Node<V> current = this;
            while (true) {
                final Queue<V> existingQueue = current.getRemainingQueue();
                final Queue<V> appendedQueue = existingQueue == null ? current.tryAppend(node) : existingQueue;
                if (appendedQueue instanceof Node<V> n) {
                    current = n;
                    continue;
                }
                return appendedQueue == null;
            }
        }

        public Queue<V> tryAppend(final Queue<V> queue) {
            @SuppressWarnings("unchecked") final Queue<V> q =
                    (Queue<V>) REMAINING_QUEUE.compareAndExchangeRelease(this, null, queue);
            return q;
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

        private final Task<?, V> parent;
        private Node<V> node;
        private boolean removeAllowed;
        private Node<V> cached;

        public QueueIterator(final Task<?, V> parent) {
            this.parent = parent;
            this.node = parent.current;
        }

        @Override
        public boolean hasNext() {
            if (this.cached != null) {
                return true;
            }
            this.cached = this.getNextNode();
            return this.cached != null;
        }

        @Override
        public V next() {
            if (this.cached == null) {
                this.cached = this.getNextNode();
            }
            this.removeAllowed = this.cached != null;
            if (this.removeAllowed) {
                this.node = this.cached;
                this.cached = this.getNextNode();
                return this.node.value;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (this.removeAllowed) {
                this.removeAllowed = false;
                this.parent.addToDoneNode(this.node);
            } else {
                throw new IllegalStateException();
            }
        }

        private Node<V> getNextNode() {
            Node<V> next = this.node;
            do {
                next = next.getRemainingQueue() instanceof Node<V> n ? n : null;
            } while (next != null && next.done);
            return next;
        }
    }

    private static sealed abstract class PartitionQueue<P, V> permits ChmPartitionQueue, SyncPartitionQueue {

        protected abstract Partition<V> getPartition(final P partitionKey, final Node<V> queue);

        protected abstract void remove(final P partitionKey);
    }

    private static final class ChmPartitionQueue<P, V> extends PartitionQueue<P, V> {

        private final ConcurrentMap<P, Partition<V>> partitions = new ConcurrentHashMap<>();

        @Override
        protected Partition<V> getPartition(final P partitionKey, final Node<V> queue) {
            final Partition<V> existing = this.partitions.get(partitionKey);
            return existing != null ? existing : this.partitions.putIfAbsent(partitionKey, new Partition<>(queue));
        }

        @Override
        protected void remove(final P partitionKey) {
            this.partitions.remove(partitionKey);
        }
    }

    private static final class SyncPartitionQueue<P, V> extends PartitionQueue<P, V> {

        private static final int MASK = 127;
        private final Map<P, Partition<V>>[] maps;

        private SyncPartitionQueue() {
            @SuppressWarnings("unchecked") final Map<P, Partition<V>>[] local = (Map<P, Partition<V>>[]) new Map<?, ?>[MASK + 1];
            for (int i = 0; i <= MASK; i++) {
                local[i] = new HashMap<>();
            }
            this.maps = local;
        }

        @Override
        protected Partition<V> getPartition(final P partitionKey, final Node<V> queue) {
            final Map<P, Partition<V>> map = this.maps[partitionKey.hashCode() & MASK];
            synchronized (map) {
                final Partition<V> existing = map.get(partitionKey);
                if (existing == null) {
                    map.put(partitionKey, new Partition<>(queue));
                }
                return existing;
            }
        }

        @Override
        protected void remove(final P partitionKey) {
            final Map<P, Partition<V>> map = this.maps[partitionKey.hashCode() & MASK];
            synchronized (map) {
                map.remove(partitionKey);
            }
        }
    }
}