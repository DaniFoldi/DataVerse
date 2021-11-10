package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.data.NamespacedStorage;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MySQLDataVerse<T> extends NamespacedDataVerse<T> {

    private final @NotNull MySQLDatabaseEngine databaseEngine;

    public MySQLDataVerse(final @NotNull MySQLDatabaseEngine databaseEngine,
                          final @NotNull String namespace,
                          final @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
        this.databaseEngine = databaseEngine;
        setup();
    }

    private void setup() {

    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> create(String key, T value) {
        return null;
    }

    @Override
    public CompletableFuture<T> get(String key) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<String>> list() {
        return null;
    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> update(String key, T value) {
        return null;
    }

    @Override
    public CompletableFuture<NamespacedStorage<T>> delete(String key) {
        return null;
    }
}