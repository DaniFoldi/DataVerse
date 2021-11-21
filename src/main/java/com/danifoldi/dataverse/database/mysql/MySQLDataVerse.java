package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

        databaseEngine.createTTLEvent(namespace);
        databaseEngine.createTable(namespace, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value) {

        return databaseEngine.create(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@Nullable T> get(String key) {
        return databaseEngine.get(namespace, key, instanceSupplier.get(), fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Collection<@NotNull String>> list() {
        return databaseEngine.list(namespace);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value) {
        return databaseEngine.update(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> delete(String key) {
        return databaseEngine.delete(namespace, key);
    }
}