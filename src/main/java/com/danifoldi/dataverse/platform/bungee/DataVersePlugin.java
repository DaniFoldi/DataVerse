package com.danifoldi.dataverse.platform.bungee;

import com.danifoldi.dataverse.data.Namespaced;
import net.md_5.bungee.api.plugin.Plugin;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class DataVersePlugin extends Plugin implements Namespaced {
    @Override
    public void onEnable() {
        super.onEnable();
    }

    @Override
    public void onDisable() {
        super.onDisable();
    }

    @Override
    public @NotNull String getNamespace() {
        return getDescription().getName();
    }
}
