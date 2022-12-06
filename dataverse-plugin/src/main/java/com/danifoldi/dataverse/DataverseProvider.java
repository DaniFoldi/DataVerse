package com.danifoldi.dataverse;

import com.danifoldi.dataverse.config.Config;
import com.danifoldi.dataverse.database.mysql.MySQLDataVerse;
import com.danifoldi.dataverse.database.mysql.MySQLDatabaseEngine;
import com.danifoldi.dataverse.database.mysql.MySQLMultiDataVerse;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DataverseProvider {

    private DataverseProvider() {

    }

    public static @NotNull CompletableFuture<@NotNull Runnable> run(final @NotNull Path configFile) {

        try {

            Config.ensureConfig(configFile);
        } catch (IOException e) {

            // todo throw
            e.printStackTrace();
        }

        Map<String, String> config = Config.getConfig(configFile);

        return DataVerse.setup(config,
                storageType -> switch (storageType) {
                    case MYSQL -> new MySQLDatabaseEngine();
                    default -> null;
                },
                (storageType, databaseEngine, namespace, instanceSupplier) -> switch (storageType) {
                    case MYSQL -> new MySQLDataVerse<>((MySQLDatabaseEngine)databaseEngine, namespace, instanceSupplier);
                    default -> null;
                },
                (storageType, databaseEngine, namespace, instanceSupplier) -> switch (storageType) {
                    case MYSQL -> new MySQLMultiDataVerse<>((MySQLDatabaseEngine)databaseEngine, namespace, instanceSupplier);
                    default -> null;
                });
    }
}
