package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.function.Supplier;

public class NamespacedDataVerse<T> {

    private final @NotNull String namespace;
    private final @NotNull Supplier<@NotNull T> instanceSupplier;

    public NamespacedDataVerse(final @NotNull String namespace,
                               final @NotNull Supplier<T> instanceSupplier) {

        this.namespace = namespace;
        this.instanceSupplier = instanceSupplier;
    }

    public void build() {

        
    }
}
