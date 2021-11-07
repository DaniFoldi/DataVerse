package com.danifoldi.dataverse.data;

import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public abstract class NamespacedDataVerse<T> implements NamespacedStorage<T> {

    protected final @NotNull String namespace;
    protected final @NotNull Supplier<@NotNull T> instanceSupplier;
    protected final @NotNull Map<@NotNull String, @NotNull FieldSpec> fieldMap = new ConcurrentHashMap<>();

    public NamespacedDataVerse(final @NotNull String namespace,
                               final @NotNull Supplier<@NotNull T> instanceSupplier) { // Could replace instancesupplier to Class<T>, right?

        this.namespace = namespace;
        this.instanceSupplier = instanceSupplier;

        buildFieldMap();
    }

    private void buildFieldMap() {

        fieldMap.clear();
        T t = instanceSupplier.get();

        for (final Field field : t.getClass().getDeclaredFields()) {

            field.setAccessible(true);
            final Type type = field.getType();
            final TypeToken<?> typeToken = TypeToken.get(type);
            final String name = field.getName();

            fieldMap.put(name, new FieldSpec(name, typeToken, field));
        }
    }
}
