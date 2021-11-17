package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.database.StorageType;
import com.danifoldi.dataverse.database.memory.MemoryDataVerse;
import com.danifoldi.dataverse.database.memory.MemoryDatabaseEngine;
import com.danifoldi.dataverse.database.mysql.MySQLDataVerse;
import com.danifoldi.dataverse.database.mysql.MySQLDatabaseEngine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DataVerse {

    private final @NotNull Map<@NotNull String, @NotNull NamespacedDataVerse<?>> cache = new ConcurrentHashMap<>();
    private @Nullable DatabaseEngine databaseEngine = null;
    private @Nullable StorageType storageType;

    private DataVerse() {

    }

    private void setup(StorageType storageType, Map<String, String> config) {

        this.storageType = storageType;

        switch (storageType) {

            case MEMORY -> {

                databaseEngine = new MemoryDatabaseEngine();
                databaseEngine.connect(config);
            }
            case MYSQL -> {

                databaseEngine = new MySQLDatabaseEngine();
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
            case MYSQL -> new MySQLDataVerse<>((MySQLDatabaseEngine)databaseEngine, namespace, instanceSupplier);
            default -> null;
        };
    }

    private void clearCache() {

        cache.clear();
    }

    private static @Nullable DataVerse instance;

    public static @Nullable DataVerse getDataVerse() {

        return instance;
    }

    public static @NotNull CompletableFuture<@NotNull Runnable> setInstance(final @NotNull StorageType storageType,
                                                                            final @NotNull Map<@NotNull String, @NotNull String> config) {

        if (instance != null) {

            // todo warn
            return CompletableFuture.failedFuture(new IllegalStateException("DataVerse instance has already been set."));
        }

        return CompletableFuture.supplyAsync(() -> {

            instance = new DataVerse();
            instance.setup(storageType, config);
            return () -> {

                if (instance.databaseEngine != null) {

                    instance.databaseEngine.close();
                }
            };
        });
    }
}
