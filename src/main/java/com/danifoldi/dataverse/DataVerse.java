package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.database.StorageType;
import com.danifoldi.dataverse.database.memory.MemoryDataVerse;
import com.danifoldi.dataverse.database.memory.MemoryDatabaseEngine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DataVerse {

    private final Map<String, NamespacedDataVerse<?>> cache = new ConcurrentHashMap<>();
    private DatabaseEngine databaseEngine = null;
    private StorageType storageType;

    private DataVerse() {

    }

    private void setup(StorageType storageType, Map<String, String> config) {
        this.storageType = storageType;

        switch (storageType) {
            case MEMORY -> {
                databaseEngine = new MemoryDatabaseEngine();
                databaseEngine.connect(config);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> @NotNull NamespacedDataVerse<@NotNull T> getNamespacedDataVerse(final @NotNull Namespaced namespaced,
                                                                               final @NotNull String name,
                                                                               final @NotNull Supplier<@NotNull T> instanceSupplier) {

        return (NamespacedDataVerse<T>)
                cache.computeIfAbsent(
                        String.format("%s:%s", namespaced.getNamespace(), name),
                        namespaceName -> createNamespacedDataVerse(namespaceName, instanceSupplier)
                );
    }

    private <T> @NotNull NamespacedDataVerse<@NotNull T> createNamespacedDataVerse(String namespace, Supplier<T> instanceSupplier) {

        return switch (storageType) {
            case MEMORY -> new MemoryDataVerse<>((MemoryDatabaseEngine)databaseEngine, namespace, instanceSupplier);
            case MYSQL -> null;
            case SQLITE -> null;
            case MARIADB -> null;
            case H2 -> null;
            case REDIS -> null;
            case MONGODB -> null;
            case FILE -> null;
        };
    }

    public void clearCache() {

        cache.clear();
    }

    private static DataVerse instance;

    public static @Nullable DataVerse getDataVerse() {

        return instance;
    }

    public static @NotNull CompletableFuture<@NotNull Runnable> setInstance(final @NotNull StorageType storageType,
                                                                        final @NotNull Map<@NotNull String, @NotNull String> config) {

        if (instance != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("DataVerse instance has already been set."));
        }

        return CompletableFuture.supplyAsync(() -> {
            instance = new DataVerse();
            instance.setup(storageType, config);
            return () -> instance.databaseEngine.close();
        });
    }
}
