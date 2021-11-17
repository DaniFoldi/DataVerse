package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.NamespacedDataVerse;
import com.danifoldi.dataverse.data.NamespacedStorage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MySQLDataVerse<T> extends NamespacedDataVerse<T> {

    private final @NotNull MySQLDatabaseEngine databaseEngine;

    public MySQLDataVerse(final @NotNull MySQLDatabaseEngine databaseEngine,
                          final @NotNull String namespace,
                          final @NotNull Supplier<@NotNull T> instanceSupplier) {

        super(namespace, instanceSupplier);
        this.databaseEngine = databaseEngine;
        setup();
    }

    private void setup() {

        databaseEngine.createTTLEvent(namespace);
        databaseEngine.createTable(namespace, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> create(String key, T value) {

        return databaseEngine.create(namespace, key, value, fieldMap);
    }

    @Override
    public @NotNull CompletableFuture<@Nullable T> get(String key) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Collection<@NotNull String>> list() {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> update(String key, T value) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<@NotNull Boolean> delete(String key) {
        return null;
    }
}