package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.data.NamespacedMultiDataVerse;
import com.danifoldi.dataverse.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MySQLMultiDataVerse<T> extends NamespacedMultiDataVerse<T> {

    private final @NotNull MySQLDatabaseEngine databaseEngine;

    public MySQLMultiDataVerse(final @NotNull MySQLDatabaseEngine databaseEngine,
                          final @NotNull String namespace,
                          final @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
        this.databaseEngine = databaseEngine;
        setup();
    }

    private void setup() {

        databaseEngine.createTTLEvent(namespace);
        databaseEngine.createMultiTable(namespace, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> add(String key, T value) {

        return databaseEngine.create(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key) {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap).thenApply(m -> m.stream().map(Pair::getSecond).toList());
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key, int pageCount, int pageLength) {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength).thenApply(m -> m.stream().map(Pair::getSecond).toList());
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength, sortKey, reverse).thenApply(m -> m.stream().map(Pair::getSecond).toList());
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
    public @NotNull CompletableFuture<@NotNull List<Pair<@NotNull String, @NotNull T>>> list() {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength) {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> delete(String key, T value) {

        return databaseEngine.deleteWhere(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> deleteAll(String key) {

        return databaseEngine.delete(namespace, key);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> expire(String key, T value, Instant expiry) {

        return databaseEngine.expireWhere(namespace, key, value, expiry, fieldMap);
    }
}
