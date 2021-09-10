package com.danifoldi.dataverse;

import com.danifoldi.dataverse.data.NamespacedDataVerse;

public class DataVerse {

    public NamespacedDataVerse getNamespacedDataStore(org.bukkit.plugin.Plugin plugin, String name) {
        plugin.getName() + "_" + name
    }

    public NamespacedDataVerse getNamespacedDataStore(net.md_5.bungee.api.plugin.Plugin plugin, String name) {

    }
}
