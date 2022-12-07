package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.data.NamespacedMultiDataVerse;
import com.danifoldi.microbase.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
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
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMin(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMin(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterMin(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterMin(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, String value) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, String value) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, String value, int pageCount, int pageLength) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, String value, int pageCount, int pageLength) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, String value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterEquals(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, String value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterEquals(namespace, key, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.filterMax(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return databaseEngine.filterMax(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterMax(namespace, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return  databaseEngine.filterMax(namespace, key, instanceSupplier, fieldMap, filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value) {

        return databaseEngine.filterBool(namespace, key, instanceSupplier, fieldMap, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value, int pageCount, int pageLength) {
        return databaseEngine.filterBool(namespace, key, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterBool(namespace, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterBool(namespace, key, instanceSupplier, fieldMap, filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix) {

        return databaseEngine.filterPrefix(namespace, key, instanceSupplier, fieldMap, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return databaseEngine.filterPrefix(namespace, key, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterPrefix(namespace, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return databaseEngine.filterPrefix(namespace, key, instanceSupplier, fieldMap, filterKey, prefix, pageCount, pageLength, sortKey, reverse);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> count() {

        return databaseEngine.count(namespace);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> count(String key) {
        return databaseEngine.count(namespace, key);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMin(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMin(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterEquals(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterEquals(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, String value) {

        return databaseEngine.countFilterEquals(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(String key, FieldSpec filterKey, String value) {

        return databaseEngine.countFilterEquals(namespace, key, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMax(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(String key, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countFilterMax(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.countFilterBool(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(String key, FieldSpec filterKey, boolean value) {

        return databaseEngine.countFilterBool(namespace, key, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.countFilterPrefix(namespace, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(String key, FieldSpec filterKey, String prefix) {

        return databaseEngine.countFilterPrefix(namespace, key, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinct() {
        return databaseEngine.countDistinct(namespace);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinct(String key) {
        return databaseEngine.countDistinct(namespace, key);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMin(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countDistinctFilterMin(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMin(String key, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.countDistinctFilterMin(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countDistinctFilterEquals(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(String key, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.countDistinctFilterEquals(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(FieldSpec filterKey, String value) {

        return databaseEngine.countDistinctFilterEquals(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(String key, FieldSpec filterKey, String value) {
        return databaseEngine.countDistinctFilterEquals(namespace, key, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMax(FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.countDistinctFilterMax(namespace, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMax(String key, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.countDistinctFilterMax(namespace, key, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterBool(FieldSpec filterKey, boolean value) {

        return databaseEngine.countDistinctFilterBool(namespace, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterBool(String key, FieldSpec filterKey, boolean value) {
        return databaseEngine.countDistinctFilterBool(namespace, key, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterPrefix(FieldSpec filterKey, String prefix) {

        return databaseEngine.countDistinctFilterPrefix(namespace, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterPrefix(String key, FieldSpec filterKey, String prefix) {
        return databaseEngine.countDistinctFilterPrefix(namespace, key, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sum(FieldSpec sumKey) {
        return databaseEngine.sum(namespace, sumKey);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sum(String key, FieldSpec sumKey) {
        return databaseEngine.sum(namespace, key, sumKey);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterMin(namespace, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.sumFilterMin(namespace, key, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterEquals(namespace, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.sumFilterEquals(namespace, key, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec sumKey, FieldSpec filterKey, String value) {

        return databaseEngine.sumFilterEquals(namespace, sumKey, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(String key, FieldSpec sumKey, FieldSpec filterKey, String value) {
        return databaseEngine.sumFilterEquals(namespace, key, sumKey, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return databaseEngine.sumFilterMax(namespace, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return databaseEngine.sumFilterMax(namespace, key, sumKey, filterKey, cutoff);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(FieldSpec sumKey, FieldSpec filterKey, boolean value) {

        return databaseEngine.sumFilterBool(namespace, sumKey, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(String key, FieldSpec sumKey, FieldSpec filterKey, boolean value) {
        return databaseEngine.sumFilterBool(namespace, key, sumKey, filterKey, value);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(FieldSpec sumKey, FieldSpec filterKey, String prefix) {

        return databaseEngine.sumFilterPrefix(namespace, sumKey, filterKey, prefix);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(String key, FieldSpec sumKey, FieldSpec filterKey, String prefix) {
        return databaseEngine.sumFilterPrefix(namespace, key, sumKey, filterKey, prefix);
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
