package com.danifoldi.dataverse.database;

import com.danifoldi.dataverse.translation.TranslationEngine;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.logging.Logger;

public interface DatabaseEngine {

    void setLogger(final @NotNull Logger logger);

    void connect(final @NotNull Map<@NotNull String, @NotNull String> config, final @NotNull TranslationEngine translationEngine);

    void close();
}
