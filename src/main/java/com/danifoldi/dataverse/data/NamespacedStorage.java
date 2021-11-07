package com.danifoldi.dataverse.data;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface NamespacedStorage<T> {

    default CompletableFuture<Boolean> exists(String key) {

        return CompletableFuture.supplyAsync(() -> get(key).join() != null);
    };

    CompletableFuture<NamespacedStorage<T>> create(String key, T value);

    CompletableFuture<T> get(String key);

    CompletableFuture<Collection<String>> list();

    default CompletableFuture<NamespacedStorage<T>> createOrUpdate(String key, T value) {

        return exists(key).thenCompose(exists -> exists ? update(key, value) : create(key, value));
    }

    CompletableFuture<NamespacedStorage<T>> update(String key, T value);

    CompletableFuture<NamespacedStorage<T>> delete(String key);



    default CompletableFuture<Boolean> exists(UUID key) {

        return exists(key.toString());
    }

    default CompletableFuture<NamespacedStorage<T>> create(UUID key, T value) {

        return create(key.toString(), value);
    }

    default CompletableFuture<T> get(UUID key) {

        return get(key.toString());
    }

    default CompletableFuture<NamespacedStorage<T>> createOrUpdate(UUID key, T value) {

        return createOrUpdate(key.toString(), value);
    }

    default CompletableFuture<NamespacedStorage<T>> update(UUID key, T value) {

        return update(key.toString(), value);
    }

    default CompletableFuture<NamespacedStorage<T>> delete(UUID key) {

        return delete(key.toString());
    }
}
