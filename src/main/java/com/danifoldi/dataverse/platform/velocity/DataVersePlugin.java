package com.danifoldi.dataverse.platform.velocity;

import com.danifoldi.dataverse.DataVerse;
import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.database.StorageType;
import com.danifoldi.dataverse.translation.TranslationEngine;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;

@Plugin(id = "dataverse",
        name = "Dataverse",
        version = "@version@",
        description = "DataVerse is an API plugin that manages data storage on many platforms.",
        authors={"DaniFoldi", "Hgex"})
public class DataVersePlugin implements Namespaced {

    private @Nullable Runnable closeDatabaseEngineConnection;

    @Subscribe
    public void onInitialize(ProxyInitializeEvent event) {

        DataVerse
                .setInstance(StorageType.MEMORY, Collections.emptyMap())
                .thenAccept(action -> closeDatabaseEngineConnection = action);
    }

    @Subscribe
    public void onShutdown(ProxyShutdownEvent event) {

        if (closeDatabaseEngineConnection != null) {

            closeDatabaseEngineConnection.run();
        }
    }

    @Override
    public @NotNull String getNamespace() {

        return "DataVerse";
    }
}
