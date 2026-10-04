package com.github.zvreifnitz.processor.impl.utils;

import java.util.Map;
import java.util.concurrent.*;

public class ExecutorUtils {

    private ExecutorUtils() {
    }

    public static Executor defaultFixedThreadPool() {
        return FixedThreadPoolProvider.EXECUTOR;
    }

    public static Executor defaultWorkStealingPool() {
        return ForkJoinPoolProvider.EXECUTOR;
    }

    public static int defaultParallelism() {
        return ParallelismProvider.PARALLELISM;
    }

    public static ExecutorInfo getInfo(final Executor executor) {
        try {
            if (executor == null) {
                return new ExecutorInfo();
            }
            final InfoCollector collector = new InfoCollector(executor);
            executor.execute(collector);
            return collector.get();
        } catch (final Exception ignored) {
            return new ExecutorInfo();
        }
    }

    private record InfoCollector(Executor executor, int depth, ConcurrentHashMap<Integer, Integer> stackLengths,
                                 ConcurrentHashMap<Integer, Boolean> virtualThreads,
                                 CountDownLatch latch) implements Runnable, Future<ExecutorInfo> {

        public InfoCollector(final Executor executor) {
            this(executor, 0, new CountDownLatch(1), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        }

        private InfoCollector(
                final Executor executor,
                final int depth,
                final CountDownLatch latch,
                final ConcurrentHashMap<Integer, Integer> stackLengths,
                final ConcurrentHashMap<Integer, Boolean> virtualThreads) {
            this(executor, depth, stackLengths, virtualThreads, latch);
        }

        private InfoCollector fork() {
            return new InfoCollector(executor, depth + 1, this.latch, this.stackLengths, this.virtualThreads);
        }

        @Override
        public void run() {
            try {
                final var thread = Thread.currentThread();
                final int stack = thread.getStackTrace().length;
                this.stackLengths.put(depth, stack);
                this.virtualThreads.put(depth, thread.isVirtual());
                if (this.depth < 3) {
                    this.executor.execute(fork());
                } else {
                    this.latch.countDown();
                }
            } catch (final Exception ignored) {
                this.latch.countDown();
            }
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return this.latch.getCount() == 0;
        }

        @Override
        public ExecutorInfo get() throws InterruptedException {
            this.latch.await();
            return this.collectInfo();
        }

        @Override
        public ExecutorInfo get(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
            if (this.latch.await(timeout, unit)) {
                return this.collectInfo();
            }
            throw new TimeoutException();
        }

        private ExecutorInfo collectInfo() {
            final Boolean overflow = this.calculateCanOverflow();
            final Boolean virtual = this.calculateIsVirtual();
            return new ExecutorInfo(
                    Boolean.FALSE.equals(overflow),
                    !Boolean.FALSE.equals(virtual));
        }

        private Boolean calculateCanOverflow() {
            if (this.stackLengths.size() != 4) {
                return null;
            }
            int maxHead = Integer.MIN_VALUE;
            int maxTail = Integer.MIN_VALUE;
            for (final Map.Entry<Integer, Integer> entry : this.stackLengths.entrySet()) {
                if (entry.getKey() < 2) {
                    maxHead = Math.max(maxHead, entry.getValue());
                } else {
                    maxTail = Math.max(maxTail, entry.getValue());
                }
            }
            return maxHead < maxTail;
        }

        private Boolean calculateIsVirtual() {
            if (this.virtualThreads.size() != 4) {
                return null;
            }
            int noCount = 0;
            int yesCount = 0;
            for (final Boolean value : this.virtualThreads.values()) {
                if (Boolean.TRUE.equals(value)) {
                    yesCount++;
                } else {
                    noCount++;
                }
            }
            return yesCount > noCount;
        }
    }

    public record DelegatingExecutor(Executor delegate) implements Executor {
        @Override
        public void execute(final Runnable command) {
            this.delegate.execute(command);
        }
    }

    private static final class ParallelismProvider {

        private static final int PARALLELISM;

        static {
            PARALLELISM = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        }
    }

    private static final class ForkJoinPoolProvider {

        private static final Executor EXECUTOR;

        static {
            EXECUTOR = createForkJoinPool();
        }

        private static Executor createForkJoinPool() {
            final int parallelism = ParallelismProvider.PARALLELISM;
            final ForkJoinPool fjp = new ForkJoinPool(parallelism,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true,
                    parallelism,
                    parallelism,
                    0,
                    p -> true,
                    Long.MAX_VALUE,
                    TimeUnit.SECONDS);
            return new DelegatingExecutor(fjp);
        }
    }

    private static final class FixedThreadPoolProvider {

        private static final Executor EXECUTOR;

        static {
            EXECUTOR = createFixedThreadPool();
        }

        private static Executor createFixedThreadPool() {
            final int parallelism = ParallelismProvider.PARALLELISM;
            final ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                    parallelism,
                    parallelism,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
            return new DelegatingExecutor(tpe);
        }
    }
}
