package com.google.common.cache;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.cache.LocalCache.DISCARDING_QUEUE;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.junit.Assert.*;


@RunWith(value = JUnit4.class)
@SuppressWarnings("deprecation")
public class LocalReloadingCacheTest {

    private static final int INITIAL_CAP = 10;
    private static final int EXPECTED_SEGMENTS = 4;

    private MapMaker createMapMaker() {
        MapMaker maker = new MapMaker();
        return maker;
    }

    // Helper methods

    private MockedCountingFunction createFunction(){
        return new MockedCountingFunction();
    }

    private <K, V>  ConcurrentMap<K, V> makeComputingMap(Function<K, V> computingFunction) {
        return createMapMaker().makeComputingMap(computingFunction);
    }

    // Tests
    @Test
    public void testInstanceOf(){
        assertTrue("Map should be "+ LocalReloadingCache.class.getName() + " type",makeComputingMap(createFunction()) instanceof LocalReloadingCache);
    }

    // computation tests
    @Test
    public void testCompute() {
        MockedCountingFunction computingFunction = createFunction();

        ConcurrentMap<Object, Object> map =
                makeComputingMap(computingFunction);
        assertEquals(0, computingFunction.getCount());

        Object key = new Object();
        Object value = map.get(key);
        assertEquals(1, computingFunction.getCount());
        assertEquals(value, map.get(key));
        assertEquals(1, computingFunction.getCount());
        // Asserting exceptions
        assertEquals(0,((LocalReloadingCache)map).globalStatsCounter.snapshot().missCount());
    }

    @Test
    public void testComputeExistingEntry() {
        MockedCountingFunction computingFunction = createFunction();
        ConcurrentMap<Object, Object> map =
                makeComputingMap(computingFunction);
        assertEquals(0, computingFunction.getCount());

        Object key = new Object();
        Object value = new Object();
        map.put(key, value);

        assertEquals(value, map.get(key));
        assertEquals(0, computingFunction.getCount());
        // Asserting exceptions
        assertEquals(0,((LocalReloadingCache)map).globalStatsCounter.snapshot().missCount());
    }

    @Test
    public void testRemovalNotification_clear() throws InterruptedException {
        // If a clear() happens while a computation is pending, we should not get a removal
        // notification.

        final CountDownLatch computingLatch = new CountDownLatch(1);
        Function<String, String> computingFunction = new DelayingIdentityLoader(computingLatch);

        final ConcurrentMap<String, String> map = new MapMaker()
                .concurrencyLevel(1)
                .makeComputingMap(computingFunction);

        // seed the map, so its segment's count > 0
        map.put("a", "a");

        final CountDownLatch computationStarted = new CountDownLatch(1);
        final CountDownLatch computationComplete = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override public void run() {
                computationStarted.countDown();
                map.get("b");
                computationComplete.countDown();
            }
        }).start();

        // wait for the computingEntry to be created
        computationStarted.await();
        map.clear();
        // let the computation proceed
        computingLatch.countDown();
        // don't check map.size() until we know the get("b") call is complete
        computationComplete.await();

        // Map should contain the computed value (b -> b), since the clear() happened before the computation
        // completed.
        assertEquals(1, map.size());
        assertEquals("b", map.get("b"));
        // Asserting exceptions
        assertEquals(0,((LocalReloadingCache)map).globalStatsCounter.snapshot().missCount());
    }

    @Test
    public void testDefaults() {
        ConcurrentMap<Object, Object> concurentMap = makeComputingMap(createFunction());
        LocalReloadingCache map = (LocalReloadingCache)concurentMap;

        assertSame(LocalCache.Strength.STRONG, map.keyStrength);
        assertSame(LocalCache.Strength.STRONG, map.valueStrength);
        assertSame(map.keyStrength.defaultEquivalence(), map.keyEquivalence);
        assertSame(map.valueStrength.defaultEquivalence(), map.valueEquivalence);

        assertEquals(0, map.expireAfterAccessNanos);
        assertEquals(0, map.expireAfterWriteNanos);

        assertSame(LocalCache.EntryFactory.STRONG, map.entryFactory);
        assertSame(DISCARDING_QUEUE, map.removalNotificationQueue);

        assertEquals(4, map.concurrencyLevel);

        // concurrency level
        assertEquals(4, map.segments.length);
        // initial capacity / concurrency level
        assertEquals(16 / map.segments.length, map.segments[0].table.length());

        assertFalse(map.evictsBySize());
        assertFalse(map.expires());
        assertFalse(map.expiresAfterWrite());
        assertFalse(map.expiresAfterAccess());
    }

    @Test
    public void testRemovalListener_replaced() {
        final CountDownLatch startSignal = new CountDownLatch(1);
        final CountDownLatch computingSignal = new CountDownLatch(1);
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final Object computedObject = new Object();

        Function<Object, Object> computingFunction = new Function<Object, Object>() {
            @Override
            public Object apply(Object key) {
                computingSignal.countDown();
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return computedObject;
            }
        };

        QueuingRemovalListener<Object, Object> listener =
                new QueuingRemovalListener<Object, Object>();
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().removalListener(listener);
        CacheLoader<Object, Object> loader = CacheLoader.from(computingFunction);


        final ConcurrentMap<Object, Object> map = builder.build(loader).asReloadingMap();
        assertTrue(listener.isEmpty());

        final Object one = new Object();
        final Object two = new Object();
        final Object three = new Object();

        new Thread() {
            @Override
            public void run() {
                map.get(one);
                assertEquals(0,((LocalReloadingCache)map).globalStatsCounter.snapshot().missCount());
                doneSignal.countDown();
            }
        }.start();

        try {
            computingSignal.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        map.put(one, two);
        startSignal.countDown();

        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertNotNull(map.putIfAbsent(one, three)); // force notifications
    }

    @Test
    @Ignore
    public void testRemovalNotification_clear_basher() throws InterruptedException {
        // If a clear() happens close to the end of computation, one of two things should happen:
        // - computation ends first: the removal listener is called, and the map does not contain the
        //   key/value pair
        // - clear() happens first: the removal listener is not called, and the map contains the pair
        CountDownLatch computationLatch = new CountDownLatch(1);
        QueuingRemovalListener<String, String> listener = new QueuingRemovalListener<String, String>();

        CacheBuilder<String, String> builder = CacheBuilder.newBuilder()
        .removalListener(listener)
        .concurrencyLevel(20);

        CacheLoader loader = CacheLoader.from(new DelayingIdentityLoader<>(computationLatch));

        @SuppressWarnings("deprecation")
        Map<String, String > map = builder.build(loader).asReloadingMap();

        int nThreads = 100;
        int nTasks = 1000;
        int nSeededEntries = 100;
        Set<String> expectedKeys = Sets.newHashSetWithExpectedSize(nTasks + nSeededEntries);
        // seed the map, so its segments have a count>0; otherwise, clear() won't visit the in-progress
        // entries
        for (int i = 0; i < nSeededEntries; i++) {
            String s = "b" + i;
            map.put(s, s);
            expectedKeys.add(s);
        }

        final AtomicInteger computedCount = new AtomicInteger();
        ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
        final CountDownLatch tasksFinished = new CountDownLatch(nTasks);
        for (int i = 0; i < nTasks; i++) {
            final String s = "a" + i;
            threadPool.submit(new Runnable() {
                @Override public void run() {
                    map.get(s);
                    computedCount.incrementAndGet();
                    tasksFinished.countDown();
                }
            });
            expectedKeys.add(s);
        }

        computationLatch.countDown();
        // let some computations complete
        while (computedCount.get() < nThreads) {
            Thread.yield();
        }
        map.clear();
        tasksFinished.await();

        // Check all of the removal notifications we received: they should have had correctly-associated
        // keys and values. (An earlier bug saw removal notifications for in-progress computations,
        // which had real keys with null values.)
        Map<String, String> removalNotifications = Maps.newHashMap();
        for (RemovalNotification<String, String> notification : listener) {
            removalNotifications.put(notification.getKey(), notification.getValue());
            assertEquals("Unexpected key/value pair passed to removalListener",
                    notification.getKey(), notification.getValue());
        }

        // All of the seed values should have been visible, so we should have gotten removal
        // notifications for all of them.
        for (int i = 0; i < nSeededEntries; i++) {
            assertEquals("b" + i, removalNotifications.get("b" + i));
        }

        // Each of the values added to the map should either still be there, or have seen a removal
        // notification.
        assertEquals(expectedKeys, Sets.union(map.keySet(), removalNotifications.keySet()));
        assertTrue(Sets.intersection(map.keySet(), removalNotifications.keySet()).isEmpty());
    }

    // Tests based on https://github.com/search?q=org%3Arichfaces4+makeComputingMap&type=Code&utf8=%E2%9C%93
    @Test
    public void testWeakKeysSoftValues(){
        ConcurrentMap map = new MapMaker().weakKeys().softValues().makeComputingMap(createFunction());
        LocalReloadingCache castedMap = (LocalReloadingCache)map;
        assertEquals(LocalCache.Strength.WEAK,castedMap.keyStrength);
        assertEquals(LocalCache.Strength.SOFT, castedMap.valueStrength);
    }

    @Test
    public void testInitialCapacity(){
        ConcurrentMap map = new MapMaker().initialCapacity(INITIAL_CAP).makeComputingMap(createFunction());
        LocalReloadingCache castedMap = (LocalReloadingCache)map;
        Assert.assertEquals(EXPECTED_SEGMENTS, castedMap.segments.length);
    }

    // computing functions

    private static class MockedCountingFunction implements Function<Object, Object> {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Object apply(Object input) {
            counter.incrementAndGet();
            return new Object();
        }

        public int getCount(){
            return counter.intValue();
        }
    }

    static final class DelayingIdentityLoader<T> implements Function<T, T> {
        private final CountDownLatch delayLatch;

        DelayingIdentityLoader(CountDownLatch delayLatch) {
            this.delayLatch = delayLatch;
        }

        @Override public T apply(T key) {
            awaitUninterruptibly(delayLatch);
            return key;
        }
    }

    static class QueuingRemovalListener<K, V>
            extends ConcurrentLinkedQueue<RemovalNotification<K, V>> implements RemovalListener<K, V> {
        @Override
        public void onRemoval(RemovalNotification<K, V> notification) {
            add(notification);
        }
    }
}