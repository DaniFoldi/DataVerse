package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class NamespacedDataVerse<T> extends FieldMappable<T> {

    public NamespacedDataVerse(@NotNull String namespace, @NotNull Supplier<@NotNull T> instanceSupplier) {
        super(namespace, instanceSupplier);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> exists(final @NotNull String key) {

        return CompletableFuture.supplyAsync(() -> get(key).join() != null);
    }

    public abstract @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value);

    public abstract @NotNull CompletableFuture<@Nullable T> get(String key);

    public @NotNull CompletableFuture<@NotNull T> getOrCreate(String key, T value) {

        return exists(key).thenCompose(exists -> {

            if (!exists) {

                return create(key, value).thenCompose(e -> get(key));
            } else {

                return get(key);
            }
        });
    }

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys();

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public abstract @NotNull CompletableFuture<@NotNull Map<@NotNull String, @NotNull T>> list();

    public abstract @NotNull CompletableFuture<@NotNull Map<@NotNull String, @NotNull T>> list(int pageCount, int pageLength);

    public abstract @NotNull CompletableFuture<@NotNull Map<@NotNull String, @NotNull T>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    public @NotNull CompletableFuture<@NotNull Boolean> createOrUpdate(String key, T value) {

        return exists(key).thenCompose(exists -> exists ? update(key, value) : create(key, value));
    }

    public abstract @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value);

    public abstract @NotNull CompletableFuture<@NotNull Boolean> delete(String key);

    public abstract @NotNull CompletableFuture<@NotNull Boolean> expire(String key, Instant expiry);


    public @NotNull CompletableFuture<@NotNull Boolean> exists(UUID key) {

        return exists(key.toString());
    }

    public @NotNull CompletableFuture<Boolean> create(UUID key, T value) {

        return create(key.toString(), value);
    }

    public @NotNull CompletableFuture<@Nullable T> get(UUID key) {

        return get(key.toString());
    }

    public @NotNull CompletableFuture<@NotNull T> getOrCreate(UUID key, T value) {

        return getOrCreate(key.toString(), value);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> createOrUpdate(UUID key, T value) {

        return createOrUpdate(key.toString(), value);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> update(UUID key, T value) {

        return update(key.toString(), value);
    }

    public @NotNull CompletableFuture<@NotNull Boolean> delete(UUID key) {

        return delete(key.toString());
    }

    public @NotNull CompletableFuture<@NotNull Boolean> expire(UUID key, Instant expiry) {

        return expire(key.toString(), expiry);
    }
}
