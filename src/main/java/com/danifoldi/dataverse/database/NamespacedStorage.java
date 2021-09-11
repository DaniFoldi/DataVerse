package com.danifoldi.dataverse.database;

public interface NamespacedStorage<T> {

    boolean exists(String key);

    void create(String key, T value);

    T get(String key);

    void createOrUpdate(String key, T value);

    void update(String key, T value);

    void delete(String key);
}
