package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface NamespacedStorage<T> {

    default @NotNull CompletableFuture<@NotNull Boolean> exists(final @NotNull String key) {

        return CompletableFuture.supplyAsync(() -> get(key).join() != null);
    }

    @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value);

    @NotNull CompletableFuture<@Nullable T> get(String key);

    default @NotNull CompletableFuture<@NotNull T> getOrCreate(String key, T value) {

        return exists(key).thenCompose(exists -> {

            if (!exists) {

                return create(key, value).thenCompose(e -> get(key));
            } else {

                return get(key);
            }
        });
    }

    @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys();

    @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength);

    @NotNull CompletableFuture<@NotNull List<@NotNull String>> keys(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    @NotNull CompletableFuture<@NotNull List<@NotNull T>> list();

    @NotNull CompletableFuture<@NotNull List<@NotNull T>> list(int pageCount, int pageLength);

    @NotNull CompletableFuture<@NotNull List<@NotNull T>> list(int pageCount, int pageLength, FieldSpec sortKey, boolean reverse);

    default @NotNull CompletableFuture<@NotNull Boolean> createOrUpdate(String key, T value) {

        return exists(key).thenCompose(exists -> exists ? update(key, value) : create(key, value));
    }

    @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value);

    @NotNull CompletableFuture<@NotNull Boolean> delete(String key);

    @NotNull CompletableFuture<@NotNull Boolean> expire(String key, Instant expiry);


    default @NotNull CompletableFuture<@NotNull Boolean> exists(UUID key) {

        return exists(key.toString());
    }

    default @NotNull CompletableFuture<Boolean> create(UUID key, T value) {

        return create(key.toString(), value);
    }

    default @NotNull CompletableFuture<@Nullable T> get(UUID key) {

        return get(key.toString());
    }

    default @NotNull CompletableFuture<@NotNull T> getOrCreate(UUID key, T value) {

        return getOrCreate(key.toString(), value);
    }

    default @NotNull CompletableFuture<@NotNull Boolean> createOrUpdate(UUID key, T value) {

        return createOrUpdate(key.toString(), value);
    }

    default @NotNull CompletableFuture<@NotNull Boolean> update(UUID key, T value) {

        return update(key.toString(), value);
    }

    default @NotNull CompletableFuture<@NotNull Boolean> delete(UUID key) {

        return delete(key.toString());
    }

    default @NotNull CompletableFuture<@NotNull Boolean> expire(UUID key, Instant expiry) {

        return expire(key.toString(), expiry);
    }
}
