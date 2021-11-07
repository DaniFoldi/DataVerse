package com.danifoldi.dataverse.database.memory;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.data.NamespacedStorage;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MemoryDataVerse<T> extends NamespacedDataVerse<T> {

    private final @NotNull MemoryDatabaseEngine databaseEngine;

    public MemoryDataVerse(final @NotNull MemoryDatabaseEngine databaseEngine,
                           final @NotNull String namespace,
                           final @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
        this.databaseEngine = databaseEngine;
    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> create(String key, T value) {

        databaseEngine.createValue(namespace, key, value);
        return CompletableFuture.supplyAsync(() -> this);
    }

    @Override
    public CompletableFuture<T> get(String key) {

        return CompletableFuture.supplyAsync(() -> (T)databaseEngine.getValue(namespace, key));
    }

    @Override
    public CompletableFuture<Collection<String>> list() {
        return CompletableFuture.supplyAsync(() -> databaseEngine.listKeys(namespace));
    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> update(String key, T value) {

        databaseEngine.updateValue(namespace, key, value);
        return CompletableFuture.supplyAsync(() -> this);
    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> delete(String key) {

        databaseEngine.deleteValue(namespace, key);
        return CompletableFuture.supplyAsync(() -> this);
    }
}
