package com.danifoldi.dataverse;

import com.danifoldi.dataverse.config.Config;
import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.database.StorageType;
import com.danifoldi.dataverse.database.mysql.MySQLDataVerse;
import com.danifoldi.dataverse.database.mysql.MySQLDatabaseEngine;
import com.danifoldi.dataverse.translation.TranslationEngine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class DataVerse {

    private final @NotNull Map<@NotNull String, @NotNull NamespacedDataVerse<?>> cache = new ConcurrentHashMap<>();
    private @Nullable DatabaseEngine databaseEngine = null;
    private @NotNull TranslationEngine translationEngine = new TranslationEngine();
    private @Nullable StorageType storageType;
    private @NotNull Logger logger = Logger.getLogger("DataVerse DatabaseEngine");

    private DataVerse() {

    }

    private void setup(Map<String, String> config) {

        this.storageType = StorageType.valueOf(config.get("storage_type").toUpperCase(Locale.ROOT));

        switch (storageType) {

            case MYSQL -> {

                databaseEngine = new MySQLDatabaseEngine();
                databaseEngine.setLogger(logger);
                databaseEngine.connect(config, translationEngine);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> @NotNull NamespacedDataVerse<@NotNull T> getNamespacedDataVerse(final @NotNull Namespaced namespaced,
                                                                               final @NotNull String name,
                                                                               final @NotNull Supplier<@NotNull T> instanceSupplier) {

        return (NamespacedDataVerse<T>)
                cache.computeIfAbsent(
                        String.format("%s_%s", namespaced.getNamespace(), name),
                        namespaceName -> createNamespacedDataVerse(namespaceName, instanceSupplier)
                );
    }

    private <T> @NotNull NamespacedDataVerse<@NotNull T> createNamespacedDataVerse(String namespace, Supplier<T> instanceSupplier) {

        return switch (storageType) {

            case MYSQL -> new MySQLDataVerse<>((MySQLDatabaseEngine)databaseEngine, namespace, instanceSupplier);
            default -> null;
        };
    }

    private void clearCache() {

        cache.clear();
    }

    public @NotNull TranslationEngine getTranslationEngine() {

        return translationEngine;
    }

    private static @Nullable DataVerse instance;

    public static @NotNull DataVerse getDataVerse() {

        if (instance == null) {

            throw new IllegalStateException("DataVerse has not yet been set up");
        }
        return instance;
    }

    public static @NotNull CompletableFuture<@NotNull Runnable> setInstance(final @NotNull Path configFile) {

        try {

            Config.ensureConfig(configFile);
        } catch (IOException e) {

            // todo throw
            e.printStackTrace();
        }

        Map<String, String> config = Config.getConfig(configFile);

        if (instance != null) {

            // todo warn
            return CompletableFuture.failedFuture(new IllegalStateException("DataVerse instance has already been set."));
        }

        return CompletableFuture.supplyAsync(() -> {

            instance = new DataVerse();
            instance.setup(config);
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
