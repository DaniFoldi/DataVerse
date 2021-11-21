package com.danifoldi.dataverse.platform.bukkit;

import com.danifoldi.dataverse.DataVerse;
import com.danifoldi.dataverse.data.Namespaced;
import com.danifoldi.dataverse.translation.TranslationEngine;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
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
                .setInstance(getDataFolder().toPath().resolve("config.dml"))
                .thenAccept(action -> {

                    closeDatabaseEngineConnection = action;

                    assert DataVerse.getDataVerse() != null;
                    TranslationEngine engine = DataVerse.getDataVerse().getTranslationEngine();

                    engine.addJavaTypeToMysqlColumn("org.bukkit.Location", "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
                    engine.addJavaTypeToMysqlColumn("org.bukkit.Material", "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
                    engine.addJavaTypeToMysqlColumn("org.bukkit.ItemStack", "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");

                    engine.addJavaTypeToMysqlQuery("org.bukkit.Location", (statement, col, field, obj) -> {

                        Location l = (Location)obj;
                        statement.setString(col, l.getWorld().getName() + "," + l.getX() + "," + l.getY() + "," + l.getZ() + "," + l.getPitch() + "," + l.getYaw());
                    });
                    engine.addJavaTypeToMysqlQuery("org.bukkit.Material", (statement, col, field, obj) -> {

                        statement.setString(col, ((Material)obj).name());
                    });
                    engine.addJavaTypeToMysqlQuery("org.bukkit.ItemStack", (statement, col, field, obj) -> {

                        YamlConfiguration config = new YamlConfiguration();
                        config.set("i", obj);
                        statement.setString(col, config.saveToString());
                    });

                    engine.addMysqlResultToJavaType("org.bukkit.Location", (results, colName, spec, obj) -> {

                        String[] parts = results.getString(colName).split(",");
                        spec.reflect().set(obj, new Location(Bukkit.getWorld(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3]), Float.parseFloat(parts[4]), Float.parseFloat(parts[5])));
                    });
                    engine.addMysqlResultToJavaType("org.bukkit.Material", (results, colName, spec, obj) -> {

                        String[] parts = results.getString(colName).split(",");
                        spec.reflect().set(obj, new Location(Bukkit.getWorld(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3]), Float.parseFloat(parts[4]), Float.parseFloat(parts[5])));
                    });
                    engine.addMysqlResultToJavaType("org.bukkit.ItemStack", (results, colName, spec, obj) -> {

                        String[] parts = results.getString(colName).split(",");
                        spec.reflect().set(obj, new Location(Bukkit.getWorld(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3]), Float.parseFloat(parts[4]), Float.parseFloat(parts[5])));
                    });

                    DataVerse
                            .getDataVerse()
                            .getNamespacedDataVerse(this, "test", UserAccount::new)
                            .create(UUID.randomUUID(), new UserAccount());
                }).join();
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
