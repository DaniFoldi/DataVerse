package com.danifoldi.dataverse.data;

import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public abstract class FieldMappable<T> {
    protected final @NotNull String namespace;
    protected final @NotNull Supplier<@NotNull T> instanceSupplier;
    protected final @NotNull Map<@NotNull String, @NotNull FieldSpec> fieldMap = new ConcurrentHashMap<>();

    public FieldMappable(final @NotNull String namespace,
                         final @NotNull Supplier<@NotNull T> instanceSupplier) {

        this.namespace = namespace;
        this.instanceSupplier = instanceSupplier;

        buildFieldMap();
    }

    private void buildFieldMap() {

        fieldMap.clear();
        final @NotNull T t = instanceSupplier.get();

        for (final Field field: t.getClass().getDeclaredFields()) {

            field.setAccessible(true);
            final @NotNull Type type = field.getType();
            @SuppressWarnings("UnstableApiUsage")
            final @NotNull TypeToken<?> typeToken = TypeToken.of(type);
            final @NotNull String name = field.getName();

            fieldMap.put(name, new FieldSpec(name, typeToken, field));
        }
    }

    public FieldSpec getField(final @NotNull String name) {

        return fieldMap.get(name);
    }
}
