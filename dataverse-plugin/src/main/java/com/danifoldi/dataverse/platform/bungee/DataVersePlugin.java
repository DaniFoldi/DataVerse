package com.danifoldi.dataverse.platform.bungee;

import com.danifoldi.dataverse.DataverseProvider;
import net.md_5.bungee.api.plugin.Plugin;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class DataVersePlugin extends Plugin {

    private @Nullable Runnable closeDatabaseEngineConnection;

    @Override
    public void onEnable() {

        DataverseProvider
                .run(getDataFolder().toPath().resolve("config.dml"))
                .thenAccept(action -> closeDatabaseEngineConnection = action);
    }

    @Override
    public void onDisable() {

        if (closeDatabaseEngineConnection != null) {

            closeDatabaseEngineConnection.run();
        }
    }
}
