package com.github.zvreifnitz.processor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.*;
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
            final ConcurrentLinkedQueue<Item> results,
            final boolean includeBuffers,
            final boolean compositeKey) {
        final OrderedProcessor<String, Item> tertiaryProcessor =
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new TertiaryWorker(results))
                        .setExtractor(compositeKey ?
                                item -> item.primaryId + "-" + item.secondaryId + "-" + item.tertiaryId
                                : item -> item.tertiaryId)
                        .setExecutor(executor)
                        .useDefaultOnClose()
                        .build();
        final OrderedProcessor<String, Item> tertiaryProcessorBuffer = includeBuffers ?
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new BufferWorker(tertiaryProcessor))
                        .setExtractor(item -> "")
                        .setExecutor(executor)
                        .setOnclose(tertiaryProcessor::close)
                        .build()
                : tertiaryProcessor;
        final OrderedProcessor<String, Item> secondaryProcessor =
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new SecondaryWorker(tertiaryProcessorBuffer))
                        .setExtractor(compositeKey ?
                                item -> item.primaryId + "-" + item.secondaryId
                                : item -> item.secondaryId)
                        .setExecutor(executor)
                        .setOnclose(tertiaryProcessorBuffer::close)
                        .build();
        final OrderedProcessor<String, Item> secondaryProcessorBuffer = includeBuffers ?
                OrderedProcessor.defaultBuilder(String.class, Item.class)
                        .setWorker(new BufferWorker(secondaryProcessor))
                        .setExtractor(item -> "")
                        .setExecutor(executor)
                        .setOnclose(secondaryProcessor::close)
                        .build()
                : secondaryProcessor;
        return OrderedProcessor.defaultBuilder(String.class, Item.class)
                .setWorker(new PrimaryWorker(secondaryProcessorBuffer))
                .setExtractor(item -> item.primaryId)
                .setExecutor(executor)
                .setOnclose(secondaryProcessorBuffer::close)
                .build();
    }

    public static void fakeWork() {
        try {
            if (ThreadLocalRandom.current().nextDouble() < 0.1) {
                Thread.yield();
            }
        } catch (final Exception ignored) {
        }
    }

    private static Stream<Arguments> testArgs() {
        final List<Arguments> result = new ArrayList<>();
        result.add(arguments(named("non-buffered", false), named("singleKey", false)));
        result.add(arguments(named("buffered", true), named("singleKey", false)));
        result.add(arguments(named("non-buffered", false), named("compositeKey", true)));
        result.add(arguments(named("buffered", true), named("compositeKey", true)));
        return result.stream();
    }

    @ParameterizedTest
    @MethodSource("testArgs")
    void orderedTest_virtual(final boolean includeBuffers, final boolean compositeKey) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        final Executor executor = Executors.newVirtualThreadPerTaskExecutor();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, results, includeBuffers, compositeKey)) {
            for (int value = 0; value < TOTAL; value++) {
                processor.enqueue(new Item(
                        "" + (value % PRIMARY),
                        "" + (value % SECONDARY),
                        "" + (value % TERTIARY),
                        value));
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
    void randomTest_virtual(final boolean includeBuffers, final boolean compositeKey) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        final Executor executor = Executors.newVirtualThreadPerTaskExecutor();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, results, includeBuffers, compositeKey)) {
            for (int value = 0; value < TOTAL; value++) {
                processor.enqueue(new Item(
                        "" + ThreadLocalRandom.current().nextInt(PRIMARY),
                        "" + ThreadLocalRandom.current().nextInt(SECONDARY),
                        "" + ThreadLocalRandom.current().nextInt(TERTIARY),
                        value));
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

    @ParameterizedTest
    @MethodSource("testArgs")
    void orderedTest_pool(final boolean includeBuffers, final boolean compositeKey) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        final Executor executor = ForkJoinPool.commonPool();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, results, includeBuffers, compositeKey)) {
            for (int value = 0; value < TOTAL; value++) {
                processor.enqueue(new Item(
                        "" + (value % PRIMARY),
                        "" + (value % SECONDARY),
                        "" + (value % TERTIARY),
                        value));
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
    void randomTest_pool(final boolean includeBuffers, final boolean compositeKey) {
        final ConcurrentLinkedQueue<Item> results = new ConcurrentLinkedQueue<>();
        final Executor executor = ForkJoinPool.commonPool();
        try (final OrderedProcessor<String, Item> processor = buildProcessor(executor, results, includeBuffers, compositeKey)) {
            for (int value = 0; value < TOTAL; value++) {
                processor.enqueue(new Item(
                        "" + ThreadLocalRandom.current().nextInt(PRIMARY),
                        "" + ThreadLocalRandom.current().nextInt(SECONDARY),
                        "" + ThreadLocalRandom.current().nextInt(TERTIARY),
                        value));
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

    public record PrimaryWorker(OrderedProcessor<String, Item> secondaryProcessor)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork();
            secondaryProcessor.enqueue(value);
        }
    }

    public record SecondaryWorker(OrderedProcessor<String, Item> tertiaryProcessor)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork();
            tertiaryProcessor.enqueue(value);
        }
    }

    public record TertiaryWorker(ConcurrentLinkedQueue<Item> results)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            fakeWork();
            results.add(value);
        }
    }

    public record BufferWorker(OrderedProcessor<String, Item> processor)
            implements OrderedProcessorWorker<String, Item> {
        @Override
        public void process(final String partition, final Item value, final Iterable<Item> remaining) {
            processor.enqueue(value);
            final Iterator<Item> iterator = remaining.iterator();
            while (iterator.hasNext()) {
                processor.enqueue(iterator.next());
                iterator.remove();
            }
        }
    }

    public record Item(String primaryId, String secondaryId, String tertiaryId, int value) {
    }
}
