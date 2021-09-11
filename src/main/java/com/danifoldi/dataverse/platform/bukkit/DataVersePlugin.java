package com.danifoldi.dataverse.platform.bukkit;

import com.danifoldi.dataverse.DataVerse;
import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.database.StorageType;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.UUID;

@SuppressWarnings("unused")
public class DataVersePlugin extends JavaPlugin implements Namespaced {

    static class UserAccount {
        UUID uuid;
        String name;
        BigDecimal balance;
    }

    private @Nullable Runnable closeDatabaseEngineConnection;

    @Override
    public void onEnable() {

        DataVerse
                .setInstance(StorageType.MEMORY, Collections.emptyMap())
                .thenAccept(action -> closeDatabaseEngineConnection = action);

        assert DataVerse.getDataVerse() != null;
        DataVerse
                .getDataVerse()
                .getNamespacedDataVerse(this, "test", UserAccount::new)
                .create(UUID.randomUUID().toString(), new UserAccount());
    }

    @Override
    public void onDisable() {

        if (closeDatabaseEngineConnection != null) {
            closeDatabaseEngineConnection.run();
        }
    }

    @Override
    public @NotNull String getNamespace() {

        return getDescription().getName();
    }
}
