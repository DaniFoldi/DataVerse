package com.danifoldi.dataverse.database.memory;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.data.NamespacedStorage;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

public class MemoryDataVerse<T> extends NamespacedDataVerse<T> {

    private final @NotNull MemoryDatabaseEngine databaseEngine;
    private final @NotNull String namespace;
    private final @NotNull Supplier<@NotNull T> instanceSupplier;

    public MemoryDataVerse(final @NotNull MemoryDatabaseEngine databaseEngine,
                           final @NotNull String namespace,
                           final @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
        this.databaseEngine = databaseEngine;
        this.namespace = namespace;
        this.instanceSupplier = instanceSupplier;
    }

    @Override
    public boolean exists(String key) {

        return databaseEngine.getValue(namespace, key) != null;
    }

    @Override
    public NamespacedStorage<T> create(String key, T value) {

        databaseEngine.createValue(namespace, key, value);
        return this;
    }

    @Override
    public T get(String key) {

        return (T)databaseEngine.getValue(namespace, key);
    }

    @Override
    public NamespacedStorage<T> createOrUpdate(String key, T value) {

        if (!exists(key)) {
            return create(key, value);
        } else {
            return update(key, value);
        }
    }

    @Override
    public NamespacedStorage<T> update(String key, T value) {

        databaseEngine.updateValue(namespace, key, value);
        return this;
    }

    @Override
    public NamespacedStorage<T> delete(String key) {

        databaseEngine.deleteValue(namespace, key);
        return this;
    }
}
