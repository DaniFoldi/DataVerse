package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import org.jetbrains.annotations.NotNull;

public class DataVerse {

    public @NotNull NamespacedDataVerse<?> getNamespacedDataStore(final @NotNull Namespaced namespaced,
                                                                  final @NotNull String name) {
        final String key = String.format("%s:%s", namespaced.getNamespace(), namespaced);
    }
}
