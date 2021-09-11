package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public abstract class NamespacedDataVerse<T> implements NamespacedStorage<T> {

    private final @NotNull String namespace;
    private final @NotNull Supplier<@NotNull T> instanceSupplier;

    private final @NotNull Map<@NotNull String, @NotNull Field> fieldMap = new ConcurrentHashMap<>();

    public NamespacedDataVerse(final @NotNull String namespace,
                               final @NotNull Supplier<T> instanceSupplier) {

        this.namespace = namespace;
        this.instanceSupplier = instanceSupplier;

        buildFieldMap();
    }

    private void buildFieldMap() {

        fieldMap.clear();
        T t = instanceSupplier.get();

        for (Field field : t.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            fieldMap.put(field.getName(), field);
        }
    }
}
