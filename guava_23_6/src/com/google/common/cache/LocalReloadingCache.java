package com.google.common.cache;

import java.util.concurrent.ExecutionException;

/**
 * Reloading local cache if value is not loaded
 *
 * @param <K> generic key type
 * @param <V> generic value type
 *
 * @author tjuodokas
 * @since 23.6-jre-e1
 * @deprecated
 * This class is custom patched for Richfaces 4.3.x. This class should never be used in EIS suite
 */

@Deprecated
class LocalReloadingCache<K ,V> extends LocalCache<K,V> {

    /**
     * Creates a new, empty map with the specified strategy, initial capacity and concurrency level.
     */

    LocalReloadingCache(CacheBuilder<? super K, ? super V> builder, CacheLoader<? super K, V> loader) {
        super(builder, loader);
    }

    /**
     * Overriding get method to allow cache update in runtime
     * @param key cache reference key
     * @return value from local cache
     *
     * @since 23.6-jre-e1
     * @deprecated
     * This method is custom patched for Richfaces 4.3.x. This class should never be used in EIS suite
     */
    @Deprecated
    @Override
    public V get(Object key) {
        V loadedValue;
        K genericKey = (K)key;

        try {
            loadedValue = super.getOrLoad(genericKey);
            if (loadedValue != null){
                super.putIfAbsent(genericKey, loadedValue);
            }
        } catch (ExecutionException e) {
            return super.getIfPresent(key);
        }
        return loadedValue;
    }
}
