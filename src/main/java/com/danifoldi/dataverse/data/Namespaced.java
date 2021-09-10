package com.danifoldi.dataverse.data;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface Namespaced {

    @NotNull String getNamespace();
}
