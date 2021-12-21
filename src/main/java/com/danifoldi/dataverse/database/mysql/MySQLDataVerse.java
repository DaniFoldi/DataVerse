package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
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
    public @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys() {
        return databaseEngine.keys(namespace);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength) {
        return databaseEngine.keys(namespace, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return databaseEngine.keys(namespace, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> list() {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> list(int pageCount, int pageLength) {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value) {
        return databaseEngine.update(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> delete(String key) {
        return databaseEngine.delete(namespace, key);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> expire(String key, Instant expiry) {
        return databaseEngine.expire(namespace, key, expiry);
    }
}