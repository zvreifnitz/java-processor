package com.github.zvreifnitz.processor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ComboTest {

    private static final int PRIMARY = 41;
    private static final int SECONDARY = 43;
    private static final int TERTIARY = 47;
    private static final int NUM_OF_ITEMS = 13;
    private static final int TOTAL = PRIMARY * SECONDARY * TERTIARY * NUM_OF_ITEMS;

    private static OrderedProcessor<String, Item> buildProcessor(
            final Executor executor,
            final int size,
            final ConcurrentLinkedQueue<Item> results,
            final boolean includeBuffers,
            final boolean compositeKey,
            final FakeWorkType workType) {
        final OrderedProcessor<String, Item> tertiaryProcessor =
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new TertiaryWorker(results, workType))
                        .setExtractor(compositeKey ?
                                item -> item.primaryId + "-" + item.secondaryId + "-" + item.tertiaryId
                                : item -> item.tertiaryId)
                        .setSize(includeBuffers ? 0 : size * 4)
                        .setExecutor(executor)
                        .useDefaultOnClose()
                        .build();
        final OrderedProcessor<String, Item> tertiaryProcessorBuffer = includeBuffers ?
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new BufferWorker(tertiaryProcessor))
                        .setExtractor(item -> "")
                        .setUnbounded()
                        .setExecutor(executor)
                        .setOnclose(tertiaryProcessor::close)
                        .build()
                : tertiaryProcessor;
        final OrderedProcessor<String, Item> secondaryProcessor =
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new SecondaryWorker(tertiaryProcessorBuffer, workType))
                        .setExtractor(compositeKey ?
                                item -> item.primaryId + "-" + item.secondaryId
                                : item -> item.secondaryId)
                        .setSize(includeBuffers ? 0 : size * 2)
                        .setExecutor(executor)
                        .setOnclose(tertiaryProcessorBuffer::close)
                        .build();
        final OrderedProcessor<String, Item> secondaryProcessorBuffer = includeBuffers ?
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new BufferWorker(secondaryProcessor))
                        .setExtractor(item -> "")
                        .setUnbounded()
                        .setExecutor(executor)
                        .setOnclose(secondaryProcessor::close)
                        .build()
                : secondaryProcessor;
        return OrderedProcessor.defaultBuilder(String.class, Item.class)
                .setWorker(new PrimaryWorker(secondaryProcessorBuffer, workType))
                .setExtractor(item -> item.primaryId)
                .setSize(size)
                .setExecutor(executor)
                .setOnclose(secondaryProcessorBuffer::close)
                .build();
    }

    private static void fakeWork(final FakeWorkType workType) {
        try {
            switch (workType) {
                case YIELD -> {
                    if (ThreadLocalRandom.current().nextDouble() < 0.1) {
                        Thread.yield();
                    }
                }
                case SLEEP -> {
                    if (ThreadLocalRandom.current().nextDouble() < 0.1) {
                        LockSupport.parkNanos(10);
                    }
                }
                default -> {
                }
            }
        } catch (final Exception ignored) {
        }
    }

    private static Stream<Arguments> testArgs() {
        return TestArgs.TEST_ARGS_LIST.stream();
    }

    @ParameterizedTest
    @MethodSource("testArgs")
    void orderedTest(
            final Executor executor,
            final int size,
            final boolean includeBuffers,
            final boolean compositeKey,
            final FakeWorkType workType) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, size, results, includeBuffers, compositeKey, workType)) {
            for (int value = 0; value < TOTAL; value++) {
                while (!processor.enqueue(new Item(
                        "" + (value % PRIMARY),
                        "" + (value % SECONDARY),
                        "" + (value % TERTIARY),
                        value))) {
                    Thread.yield();
                }
            }
        }

        assertEquals(TOTAL, results.size());
        final Map<String, List<Integer>> checkLists = new HashMap<>();
        for (final var item : results) {
            final List<Integer> list = checkLists.computeIfAbsent(
                    item.primaryId + "-" + item.secondaryId + "-" + item.tertiaryId,
                    k -> new ArrayList<>());
            list.add(item.value);
        }
        for (final var list : checkLists.values()) {
            assertEquals(NUM_OF_ITEMS, list.size());
            for (int i = 1; i < list.size(); i++) {
                assertTrue(list.get(i - 1) < list.get(i));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testArgs")
    void randomTest(
            final Executor executor,
            final int size,
            final boolean includeBuffers,
            final boolean compositeKey,
            final FakeWorkType workType) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, size, results, includeBuffers, compositeKey, workType)) {
            for (int value = 0; value < TOTAL; value++) {
                while (!processor.enqueue(new Item(
                        "" + ThreadLocalRandom.current().nextInt(PRIMARY),
                        "" + ThreadLocalRandom.current().nextInt(SECONDARY),
                        "" + ThreadLocalRandom.current().nextInt(TERTIARY),
                        value))) {
                    Thread.yield();
                }
            }
        }

        assertEquals(TOTAL, results.size());
        final Map<String, List<Integer>> checkLists = new HashMap<>();
        for (final var item : results) {
            final List<Integer> list = checkLists.computeIfAbsent(
                    item.primaryId + "-" + item.secondaryId + "-" + item.tertiaryId,
                    k -> new ArrayList<>());
            list.add(item.value);
        }
        for (final var list : checkLists.values()) {
            for (int i = 1; i < list.size(); i++) {
                assertTrue(list.get(i - 1) < list.get(i));
            }
        }
    }

    enum FakeWorkType {
        NONE,
        YIELD,
        SLEEP
    }

    record PrimaryWorker(OrderedProcessor<String, Item> secondaryProcessor, FakeWorkType workType)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork(workType);
            while (!secondaryProcessor.enqueue(value)) {
                Thread.yield();
            }
        }
    }

    record SecondaryWorker(OrderedProcessor<String, Item> tertiaryProcessor, FakeWorkType workType)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork(workType);
            while (!tertiaryProcessor.enqueue(value)) {
                Thread.yield();
            }
        }
    }

    record TertiaryWorker(ConcurrentLinkedQueue<Item> results, FakeWorkType workType)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork(workType);
            results.add(value);
        }
    }

    record BufferWorker(OrderedProcessor<String, Item> processor)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            while (!processor.enqueue(value)) {
                Thread.yield();
            }
            final Iterator<Item> iterator = remaining.iterator();
            while (iterator.hasNext()) {
                if (processor.enqueue(iterator.next())) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }

    record Item(String primaryId, String secondaryId, String tertiaryId, int value) {
    }

    private static class TestArgs {
        private final static List<Arguments> TEST_ARGS_LIST;

        static {
            final List<Arguments> result = new ArrayList<>();
            for (final var executorArg : List.of(
                    named("sameThread", (Executor) Runnable::run),
                    named("singleThread", Executors.newSingleThreadExecutor()),
                    named("threadPool", Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())),
                    named("virtualThreadPool", Executors.newVirtualThreadPerTaskExecutor()),
                    named("forkJoinPool", ForkJoinPool.commonPool())))
                for (final var sizeArg : List.of(
                        named("unbounded", 0),
                        named("bounded", 1)))
                    for (final var bufferedArg : List.of(
                            named("non-buffered", false),
                            named("buffered", true)))
                        for (final var compositeArg : List.of(
                                named("singleKey", false),
                                named("compositeKey", true)))
                            for (final var workTypeArg : Arrays.stream(FakeWorkType.values())
                                    .map(wt -> named(wt.name().toLowerCase(), wt))
                                    .toList())
                                result.add(arguments(executorArg, sizeArg, bufferedArg, compositeArg, workTypeArg));
            TEST_ARGS_LIST = result;
        }
    }
}
