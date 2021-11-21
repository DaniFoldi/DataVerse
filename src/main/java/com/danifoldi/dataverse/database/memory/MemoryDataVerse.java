package com.danifoldi.dataverse.database.memory;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    public @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value) {

        databaseEngine.createValue(namespace, key, value);
        return CompletableFuture.supplyAsync(() -> true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull CompletableFuture<@Nullable T> get(String key) {

        return CompletableFuture.supplyAsync(() -> (T)databaseEngine.getValue(namespace, key));
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Collection<@NotNull String>> list() {

        return CompletableFuture.supplyAsync(() -> databaseEngine.listKeys(namespace));
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value) {

        databaseEngine.updateValue(namespace, key, value);
        return CompletableFuture.supplyAsync(() -> true);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> delete(String key) {

        databaseEngine.deleteValue(namespace, key);
        return CompletableFuture.supplyAsync(() -> true);
    }
}
