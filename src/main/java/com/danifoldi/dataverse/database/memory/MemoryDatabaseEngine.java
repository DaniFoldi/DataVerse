package com.danifoldi.dataverse.database.memory;

import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.translation.TranslationEngine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class MemoryDatabaseEngine implements DatabaseEngine {

    private final @NotNull Map<@NotNull String, @NotNull Map<@NotNull String, @NotNull Object>> storage = new ConcurrentHashMap<>();

    @Override
    public void setLogger(@NotNull Logger logger) {

    }

    @Override
    public void connect(@NotNull Map<@NotNull String, @NotNull String> config, final @NotNull TranslationEngine translationEngine) {

        storage.clear();
    }

    @Override
    public void close() {

    }

    @Nullable Object getValue(String namespace, String key) {

        return storage.get(namespace).get(key);
    }

    @NotNull Collection<@NotNull String> listKeys(String namespace) {

        return storage.get(namespace).keySet();
    }

    void createValue(String namespace, String key, Object value) {

        storage.get(namespace).put(key, value);
    }

    void updateValue(String namespace, String key, Object value) {

        storage.get(namespace).put(key, value);
    }

    void deleteValue(String namespace, String key) {

        storage.get(namespace).remove(key);
    }
}
