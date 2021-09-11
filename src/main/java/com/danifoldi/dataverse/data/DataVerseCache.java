package com.danifoldi.dataverse.data;

import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DataVerseCache {

    private final Map<String, NamespacedDataVerse<?>> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> NamespacedDataVerse<T> get(String namespace, Class<T> clazz) {

        return (NamespacedDataVerse<T>) cache.computeIfAbsent(namespace, namespaceName -> new NamespacedDataVerse<>(namespaceName, clazz));
    }
}
