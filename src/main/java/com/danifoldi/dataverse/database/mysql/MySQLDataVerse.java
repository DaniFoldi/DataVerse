package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.microbase.util.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
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
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list() {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength) {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.list(namespace, instanceSupplier, fieldMap, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMin(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.countFilterMin(namespace,  filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.countFilterMin(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterEquals(namespace,  filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.countFilterEquals(namespace, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.countFilterEquals(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMax(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.countFilterMax(namespace, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.countFilterMax(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.countFilterBool(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return databaseEngine.countFilterBool(namespace, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.countFilterBool(namespace, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.countFilterPrefix(namespace, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return databaseEngine.countFilterPrefix(namespace, filterKey, prefix, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.countFilterPrefix(namespace, filterKey, prefix, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterMin(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.sumFilterMin(namespace, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.sumFilterMin(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterEquals(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.sumFilterEquals(namespace, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.sumFilterEquals(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterMax(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.sumFilterMax(namespace, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.sumFilterMax(namespace, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.sumFilterBool(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return databaseEngine.sumFilterBool(namespace, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.sumFilterBool(namespace, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.sumFilterPrefix(namespace, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return databaseEngine.sumFilterPrefix(namespace, filterKey, prefix, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.sumFilterPrefix(namespace, filterKey, prefix, pageCount, pageLength, sortKey, reverse);
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