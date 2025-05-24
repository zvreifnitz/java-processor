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

    public static Boolean canStackOverflow(final Executor executor) {
        try {
            if (executor == null) {
                return null;
            }
            final StackFrameCountCheck check = new StackFrameCountCheck(executor);
            executor.execute(check);
            return check.get();
        } catch (final Exception ignored) {
            return null;
        }
    }

    private static final class StackFrameCountCheck implements Runnable, Future<Boolean> {

        private final Executor executor;
        private final int depth;
        private final ConcurrentHashMap<Integer, Integer> result;
        private final CountDownLatch latch;

        public StackFrameCountCheck(final Executor executor) {
            this(executor, 0, new CountDownLatch(1), new ConcurrentHashMap<>());
        }

        private StackFrameCountCheck(
                final Executor executor,
                final int depth,
                final CountDownLatch latch,
                final ConcurrentHashMap<Integer, Integer> result) {
            this.executor = executor;
            this.depth = depth;
            this.latch = latch;
            this.result = result;
        }

        private StackFrameCountCheck fork() {
            return new StackFrameCountCheck(executor, depth + 1, this.latch, this.result);
        }

        @Override
        public void run() {
            try {
                final int stack = Thread.currentThread().getStackTrace().length;
                this.result.put(depth, stack);
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
        public Boolean get() throws InterruptedException {
            this.latch.await();
            return this.checkStackCounts();
        }

        @Override
        public Boolean get(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
            if (this.latch.await(timeout, unit)) {
                return this.checkStackCounts();
            }
            throw new TimeoutException();
        }

        private Boolean checkStackCounts() {
            if (this.result.size() != 4) {
                return null;
            }
            int maxHead = Integer.MIN_VALUE;
            int maxTail = Integer.MIN_VALUE;
            for (final Map.Entry<Integer, Integer> entry : this.result.entrySet()) {
                if (entry.getKey() < 2) {
                    maxHead = Math.max(maxHead, entry.getValue());
                } else {
                    maxTail = Math.max(maxTail, entry.getValue());
                }
            }
            return maxHead < maxTail;
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
            PARALLELISM = (Runtime.getRuntime().availableProcessors() + 1) / 2;
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
