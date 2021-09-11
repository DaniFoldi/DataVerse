package com.danifoldi.dataverse.data;

public interface NamespacedStorage<T> {

    boolean exists(String key);

    NamespacedStorage<T> create(String key, T value);

    T get(String key);

    NamespacedStorage<T> createOrUpdate(String key, T value);

    NamespacedStorage<T> update(String key, T value);

    NamespacedStorage<T> delete(String key);
}
