package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.DataVerseCache;
import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.data.NamespacedDataVerse;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DataVerse {

    private final DataVerseCache cache;

    @Inject
    public DataVerse(final @NotNull DataVerseCache cache) {
        
        this.cache = cache;
    }

    public <T> @NotNull NamespacedDataVerse<@NotNull T> getNamespacedDataStore(final @NotNull Namespaced namespaced,
                                                                               final @NotNull String name, final Class<T> clazz) {

        return cache.get(String.format("%s:%s", namespaced.getNamespace(), name), clazz);
    }
}
