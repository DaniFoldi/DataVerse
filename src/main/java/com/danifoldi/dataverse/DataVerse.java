package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.data.NamespacedMultiDataVerse;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.database.StorageType;
import com.danifoldi.dataverse.translation.TranslationEngine;
import com.danifoldi.dataverse.util.QuadFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class DataVerse {

    private final @NotNull Map<@NotNull String, @NotNull NamespacedDataVerse<?>> cache = new ConcurrentHashMap<>();
    private final @NotNull Map<@NotNull String, @NotNull NamespacedMultiDataVerse<?>> multiCache = new ConcurrentHashMap<>();
    private QuadFunction<StorageType, DatabaseEngine, String, Supplier<?>, NamespacedDataVerse<?>> dataverseProvider;
    private QuadFunction<StorageType, DatabaseEngine, String, Supplier<?>, NamespacedMultiDataVerse<?>> multiDataverseProvider;
    private @Nullable DatabaseEngine databaseEngine = null;
    private final @NotNull TranslationEngine translationEngine = new TranslationEngine();
    public @Nullable StorageType storageType;
    private final @NotNull Logger logger = Logger.getLogger("DataVerse");

    private static DataVerse instance;

    public DataVerse() {

    }

    @SuppressWarnings("unchecked, unused")
    public <T> @NotNull NamespacedDataVerse<@NotNull T> getNamespacedDataVerse(final @NotNull Namespaced namespaced,
                                                                               final @NotNull String name,
                                                                               final @NotNull Supplier<@NotNull T> instanceSupplier) {

        return (NamespacedDataVerse<T>)
                cache.computeIfAbsent(
                        String.format("%s_%s", namespaced.getNamespace(), name),
                        namespaceName -> createNamespacedDataVerse(namespaceName, instanceSupplier)
                );
    }

    @SuppressWarnings("unchecked, unused")
    public <T> @NotNull NamespacedMultiDataVerse<@NotNull T> getNamespacedMultiDataVerse(final @NotNull Namespaced namespaced,
                                                                               final @NotNull String name,
                                                                               final @NotNull Supplier<@NotNull T> instanceSupplier) {

        return (NamespacedMultiDataVerse<T>)
                multiCache.computeIfAbsent(
                        String.format("%s_%s", namespaced.getNamespace(), name),
                        namespaceName -> createNamespacedMultiDataVerse(namespaceName, instanceSupplier)
                );
    }

    private <T> @NotNull NamespacedDataVerse<@NotNull T> createNamespacedDataVerse(String namespace, Supplier<T> instanceSupplier) {

        if (storageType == null || databaseEngine == null) {

            logger.severe("Setup has not been called before requesting a dataverse");
            throw new IllegalStateException("Setup has not been called before requesting a dataverse");
        }

        //noinspection unchecked
        return (NamespacedDataVerse<T>)dataverseProvider.apply(storageType, databaseEngine, namespace, instanceSupplier);
    }

    private <T> @NotNull NamespacedMultiDataVerse<@NotNull T> createNamespacedMultiDataVerse(String namespace, Supplier<T> instanceSupplier) {

        if (storageType == null || databaseEngine == null) {

            logger.severe("Setup has not been called before requesting a dataverse");
            throw new IllegalStateException("Setup has not been called before requesting a dataverse");
        }

        //noinspection unchecked
        return (NamespacedMultiDataVerse<T>)multiDataverseProvider.apply(storageType, databaseEngine, namespace, instanceSupplier);
    }

    @SuppressWarnings("unused")
    private void clearCache() {

        cache.clear();
        multiCache.clear();
    }

    public @NotNull TranslationEngine getTranslationEngine() {

        return translationEngine;
    }

    public static @NotNull DataVerse getDataVerse() {

        if (instance == null) {

            throw new IllegalStateException("DataVerse has not yet been set up");
        }
        return instance;
    }

    public static @NotNull CompletableFuture<@NotNull Runnable> setup(Map<String, String> config,
                                                                      Function<StorageType, DatabaseEngine> databaseEngineProvider,
                                                                      QuadFunction<StorageType, DatabaseEngine, String, Supplier<?>, NamespacedDataVerse<?>> dataverseProvider,
                                                                      QuadFunction<StorageType, DatabaseEngine, String, Supplier<?>, NamespacedMultiDataVerse<?>> multiDataverseProvider) {

        if (instance != null) {

            // todo warn
            return CompletableFuture.failedFuture(new IllegalStateException("DataVerse instance has already been set."));
        }

        return CompletableFuture.supplyAsync(() -> {

            instance = new DataVerse();

            instance.storageType = StorageType.valueOf(config.get("storage_type").toUpperCase(Locale.ROOT));
            instance.databaseEngine = databaseEngineProvider.apply(instance.storageType);
            instance.dataverseProvider = dataverseProvider;
            instance.multiDataverseProvider = multiDataverseProvider;
            instance.databaseEngine.setLogger(instance.logger);
            instance.databaseEngine.connect(config, instance.translationEngine);

            instance.translationEngine.clear();
            instance.translationEngine.setupStandard();
            return () -> {

                if (instance.databaseEngine != null) {

                    instance.databaseEngine.close();
                }
            };
        });
    }
}
