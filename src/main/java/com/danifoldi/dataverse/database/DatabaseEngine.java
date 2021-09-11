package com.danifoldi.dataverse.database;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface DatabaseEngine {

    void connect(final @NotNull Map<@NotNull String, @NotNull String> config);

    void close();
}
