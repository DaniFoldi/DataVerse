package com.danifoldi.dataverse.data;

import com.danifoldi.microbase.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
