package com.danifoldi.dataverse.data;

import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DataVerseCache {
    private final Map<String, NamespacedDataVerse<Class>> cache = new ConcurrentHashMap<>();

    public NamespacedDataVerse<Class> get(String key) {
        if (!cache.containsKey(key)) {
            cache.putIfAbsent(key, new NamespacedDataVerse<>(key));
        }
    }
}
