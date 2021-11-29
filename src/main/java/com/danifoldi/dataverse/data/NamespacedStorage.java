package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface NamespacedStorage<T> {

    default @NotNull CompletableFuture<@NotNull Boolean> exists(final @NotNull String key) {

        return CompletableFuture.supplyAsync(() -> get(key).join() != null);
    }

    @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value);

    @NotNull CompletableFuture<@Nullable T> get(String key);

    @NotNull CompletableFuture<@NotNull Collection<@NotNull String>> list();

    default @NotNull CompletableFuture<@NotNull Boolean> createOrUpdate(String key, T value) {

        return exists(key).thenCompose(exists -> exists ? update(key, value) : create(key, value));
    }

    @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value);

    @NotNull CompletableFuture<@NotNull Boolean> delete(String key);



    default @NotNull CompletableFuture<@NotNull Boolean> exists(UUID key) {

        return exists(key.toString());
    }

    default @NotNull CompletableFuture<Boolean> create(UUID key, T value) {

        return create(key.toString(), value);
    }

    default @NotNull CompletableFuture<@Nullable T> get(UUID key) {

        return get(key.toString());
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
}
