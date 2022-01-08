package com.danifoldi.dataverse.platform.velocity;

import com.danifoldi.dataverse.DataVerse;
import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;

@Plugin(id = "dataverse",
        name = "Dataverse",
        version = "@version@",
        description = "DataVerse is an API plugin that manages data storage on many platforms.",
        authors={"DaniFoldi", "Hgex"})
public class DataVersePlugin {

    private @Nullable Runnable closeDatabaseEngineConnection;
    private @NotNull Path datafolder;

    @Inject
    public DataVersePlugin(final @DataDirectory @NotNull Path datafolder) {

        this.datafolder = datafolder;
    }

    @Subscribe
    public void onInitialize(ProxyInitializeEvent event) {

        DataVerse
                .setup(datafolder.resolve("config.dml"))
                .thenAccept(action -> closeDatabaseEngineConnection = action);
    }

    @Subscribe
    public void onShutdown(ProxyShutdownEvent event) {

        if (closeDatabaseEngineConnection != null) {

            closeDatabaseEngineConnection.run();
        }
    }
}
