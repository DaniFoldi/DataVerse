package com.danifoldi.dataverse.database;

import com.danifoldi.dataverse.translation.TranslationEngine;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface DatabaseEngine {

    void connect(final @NotNull Map<@NotNull String, @NotNull String> config, TranslationEngine translationEngine);

    void close();
}
