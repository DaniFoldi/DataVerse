package com.danifoldi.dataverse.platform.bukkit;

import com.danifoldi.dataverse.data.Namespaced;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class DataVersePlugin extends JavaPlugin implements Namespaced {

    @Override
    public void onEnable() {

    }

    @Override
    public void onDisable() {

    }

    @Override
    public @NotNull String getNamespace() {
        return getDescription().getName();
    }
}
