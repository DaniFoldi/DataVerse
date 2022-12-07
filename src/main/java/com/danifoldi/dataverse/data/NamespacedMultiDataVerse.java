package com.danifoldi.dataverse.data;

import com.danifoldi.microbase.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public abstract class NamespacedMultiDataVerse<T> extends FieldMappable<T> {

    public NamespacedMultiDataVerse(@NotNull String namespace, @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> empty(final @NotNull String key) {

        return CompletableFuture.supplyAsync(() -> get(key).join().isEmpty());
    }

    public abstract @NotNull CompletableFuture<@NotNull Boolean> add(String key, T value);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(String key, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys();

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list();

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull Boolean> delete(String key, T value);

    public abstract @NotNull CompletableFuture<@NotNull Boolean> deleteAll(String key);

    public abstract @NotNull CompletableFuture<@NotNull Boolean> expire(String key, T value, Instant expiry);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return filterMin(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {
        return filterMin(key.toString(), filterKey, cutoff, pageCount, pageLength);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMin(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return filterMin(key.toString(), filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return filterEquals(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {
        return filterEquals(key.toString(), filterKey, cutoff,pageCount, pageLength);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterEquals(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return filterEquals(key.toString(), filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return filterMax(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {
        return filterMax(key.toString(), filterKey, cutoff,pageCount, pageLength);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(String key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterMax(UUID key, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return filterMax(key.toString(), filterKey, cutoff, pageCount, pageLength, sortKey, reverse);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(UUID key, FieldSpec filterKey, boolean value) {
        return filterBool(key.toString(), filterKey, value);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value, int pageCount, int pageLength);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(UUID key, FieldSpec filterKey, boolean value, int pageCount, int pageLength) {
        return filterBool(key.toString(), filterKey, value, pageCount, pageLength);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(String key, FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterBool(UUID key, FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return filterBool(key.toString(), filterKey, value, pageCount, pageLength, sortKey, reverse);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(UUID key, FieldSpec filterKey, String prefix) {
        return filterPrefix(key.toString(), filterKey, prefix);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix, int pageCount, int pageLength);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(UUID key, FieldSpec filterKey, String prefix, int pageCount, int pageLength) {
        return filterPrefix(key.toString(), filterKey, prefix, pageCount, pageLength);
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(String key, FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull List<@NotNull Pair<@NotNull String, @NotNull T>>> filterPrefix(UUID key, FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {
        return filterPrefix(key.toString(), filterKey, prefix, pageCount, pageLength, sortKey, reverse);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> count();

    public abstract @NotNull CompletableFuture<@NotNull Long> count(String key);

    public @NotNull CompletableFuture<@NotNull Long> count(UUID key) {
        return count(key.toString());
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterMin(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterMin(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countFilterMin(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countFilterMin(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterEquals(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterEquals(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countFilterEquals(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countFilterEquals(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterMax(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterMax(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countFilterMax(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countFilterMax(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterBool(FieldSpec filterKey, boolean value);

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterBool(String key, FieldSpec filterKey, boolean value);

    public @NotNull CompletableFuture<@NotNull Long> countFilterBool(UUID key, FieldSpec filterKey, boolean value) {
        return countFilterBool(key.toString(), filterKey, value);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(FieldSpec filterKey, String prefix);

    public abstract @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(String key, FieldSpec filterKey, String prefix);

    public @NotNull CompletableFuture<@NotNull Long> countFilterPrefix(UUID key, FieldSpec filterKey, String prefix) {
        return countFilterPrefix(key.toString(), filterKey, prefix);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinct();

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinct(String key);

    public @NotNull CompletableFuture<@NotNull Long> countDistinct(UUID key) {
        return countDistinct(key.toString());
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMin(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMin(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMin(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countDistinctFilterMin(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterEquals(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countDistinctFilterEquals(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMax(FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMax(String key, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterMax(UUID key, FieldSpec filterKey, BigDecimal cutoff) {
        return countDistinctFilterMax(key.toString(), filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterBool(FieldSpec filterKey, boolean value);

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterBool(String key, FieldSpec filterKey, boolean value);

    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterBool(UUID key, FieldSpec filterKey, boolean value) {
        return countDistinctFilterBool(key.toString(), filterKey, value);
    }

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterPrefix(FieldSpec filterKey, String prefix);

    public abstract @NotNull CompletableFuture<@NotNull Long> countDistinctFilterPrefix(String key, FieldSpec filterKey, String prefix);

    public @NotNull CompletableFuture<@NotNull Long> countDistinctFilterPrefix(UUID key, FieldSpec filterKey, String prefix) {
        return countDistinctFilterPrefix(key.toString(), filterKey, prefix);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sum(FieldSpec sumKey);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sum(String key, FieldSpec sumKey);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sum(UUID key, FieldSpec sumKey) {
        return sum(key.toString(), sumKey);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMin(UUID key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return sumFilterMin(key.toString(), sumKey, filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterEquals(UUID key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return sumFilterEquals(key.toString(), sumKey, filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterMax(UUID key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {
        return sumFilterMax(key.toString(), sumKey, filterKey, cutoff);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(FieldSpec sumKey, FieldSpec filterKey, boolean value);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(String key, FieldSpec sumKey, FieldSpec filterKey, boolean value);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterBool(UUID key, FieldSpec sumKey, FieldSpec filterKey, boolean value) {
        return sumFilterBool(key.toString(), sumKey, filterKey, value);
    }

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(FieldSpec sumKey, FieldSpec filterKey, String prefix);

    public abstract @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(String key, FieldSpec sumKey, FieldSpec filterKey, String prefix);

    public @NotNull CompletableFuture<@NotNull BigDecimal> sumFilterPrefix(UUID key, FieldSpec sumKey, FieldSpec filterKey, String prefix) {
        return sumFilterPrefix(key.toString(), sumKey, filterKey, prefix);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> empty(UUID key) {

        return empty(key.toString());
    }

    public @NotNull CompletableFuture<Boolean> add(UUID key, T value) {

        return add(key.toString(), value);
    }

    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(UUID uuid) {

        return get(uuid.toString());
    }

    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(UUID uuid, int pageCount, int pageLength) {

        return get(uuid.toString(), pageCount, pageLength);
    }

    public @NotNull CompletableFuture<@NotNull List<@NotNull T>> get(UUID uuid, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return get(uuid.toString(), pageCount, pageLength, sortKey, reverse);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> delete(UUID uuid, T value) {

        return delete(uuid.toString(), value);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> deleteAll(UUID uuid) {

        return deleteAll(uuid.toString());
    }

    public @NotNull CompletableFuture<@NotNull Boolean> expire(UUID uuid, T value, Instant expiry) {

        return expire(uuid.toString(), value, expiry);
    }
}
