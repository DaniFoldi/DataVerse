package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MySQLDatabaseEngine implements DatabaseEngine {

    HikariPool connectionPool;

    @Override
    public void connect(@NotNull Map<@NotNull String, @NotNull String> config) {

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setAllowPoolSuspension(false);
        hikariConfig.setIdleTimeout(30000);
        hikariConfig.setConnectionTimeout(5000);
        hikariConfig.setInitializationFailTimeout(5000);
        hikariConfig.setKeepaliveTime(2000);
        hikariConfig.setLeakDetectionThreshold(30000);
        hikariConfig.setMaximumPoolSize(16);
        hikariConfig.setMaxLifetime(3600000);
        hikariConfig.setPoolName("DataVerse Hikari MySQL Pool");
        hikariConfig.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfig.setUsername(config.get("username"));
        hikariConfig.setPassword(config.get("password"));
        hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?%s", config.get("host"), config.get("port"), config.get("database"), config.get("options").replaceFirst("^\\?", "")));
        hikariConfig.addDataSourceProperty("cachePrepStmts", true);
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", 200);
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", 1024);
        hikariConfig.addDataSourceProperty("cacheResultSetMetadata", true);
        hikariConfig.addDataSourceProperty("cacheServerConfiguration", true);
        hikariConfig.addDataSourceProperty("useServerPrepStmts", true);
        hikariConfig.addDataSourceProperty("useLocalSessionState", true);
        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", true);
        hikariConfig.addDataSourceProperty("maintainTimeStats", false);

        try {

            if (connectionPool != null) {

                connectionPool.shutdown();
            }
        } catch (InterruptedException e) {

            // todo warn
        }

        connectionPool = new HikariPool(hikariConfig);
    }

    @Override
    public void close() {

        try {

            if (connectionPool != null) {

                connectionPool.shutdown();
            }
        } catch (InterruptedException e) {

            // todo warn
        }
    }

    void createTTLEvent(String namespace) {

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement("""
                    CREATE EVENT IF NOT EXISTS
                      `%s`
                    ON SCHEDULE EVERY 1 HOUR
                    DO
                    DELETE FROM
                      `%s`
                    WHERE `%s` < NOW();
             """.formatted(eventName(namespace, "ttl"), tableName(namespace), columnName("ttl_timestamp")))) {

            statement.execute();
        } catch (SQLException e) {

            // todo error
        }
    }

    private String tableName(String namespace) {

        return "%s_dataverse".formatted(namespace).toLowerCase(Locale.ROOT).replace("\s", "");
    }

    private String eventName(String namespace, String event) {

        return "%s_%s_dataverse".formatted(namespace, event).toLowerCase(Locale.ROOT).replace("\s", "");
    }

    private String columnName(String name) {

        return "dataverse_%s".formatted(name).toLowerCase(Locale.ROOT).replace("\s", "");
    }

    private String columnName(String type, String name) {

        return "%s_%s".formatted(type, name).toLowerCase(Locale.ROOT).replace("\s", "");
    }

    @SuppressWarnings("unchecked")
    private String columnType(FieldSpec spec) {

        String typeName = spec.reflect().getDeclaringClass().getName();
        return switch (typeName) {

            case "java.lang.String" -> "VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "int", "java.lang.Integer" -> "INT";
            case "byte", "java.lang.Byte" -> "TINYINT";
            case "long", "java.lang.Long" -> "BIGINT";
            case "short", "java.lang.Short" -> "SMALLINT";
            case "float", "java.lang.Float" -> "FLOAT";
            case "double", "java.lang.Double" -> "DOUBLE";
            case "boolean", "java.lang.Boolean" -> "BOOLEAN";
            case "char", "java.lang.Char" -> "CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "org.bukkit.Location" -> "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "org.bukkit.Material" -> "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "org.bukkit.ItemStack" -> "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "java.math.BigDecimal" -> "DECIMAL";
            case "java.util.UUID" -> "VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            case "java.util.List" ->

                    // todo list generic data
                    // ((Class)(((ParameterizedType)(spec.reflect().getDeclaringClass().getGenericSuperclass())).getActualTypeArguments()[0])).getTypeName();
                    "JSON";
            default ->

                    // todo throw
                    "";
        };

    }

    void createTable(String namespace, Map<String, FieldSpec> fieldMap) {

        StringBuilder columns = new StringBuilder();
        fieldMap.forEach((name, spec) -> columns.append("`%s` %s,\n".formatted(columnName(spec.reflect().getDeclaringClass().getSimpleName(), name), columnType(spec))));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement("""
                    CREATE TABLE IF NOT EXISTS
                    `%s`
                     (
                     `%s` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                     `%s` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     `%s` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     `%s` TIMESTAMP NULL DEFAULT NULL,
                     %s
                     
                     PRIMARY KEY (`%s`)
                     )
                     ENGINE = InnoDB
                     CHARSET = utf8mb4
                     COLLATE utf8mb4_unicode_ci;
             """.formatted(tableName(namespace), columnName("key"), columnName("create_timestamp"), columnName("update_timestamp"), columnName("ttl_timestamp"), columns.toString(), columnName("key")))) {

            statement.execute();
        } catch (SQLException e) {

            // todo error
        }
    }

    CompletableFuture<Boolean> create(String namespace, String key, Object value, Map<String, FieldSpec> fieldMap) {
        return CompletableFuture.supplyAsync(() -> {

            StringBuilder columns = new StringBuilder();
            fieldMap.forEach((name, spec) -> columns.append("`%s` %s,\n".formatted(columnName(spec.reflect().getDeclaringClass().getSimpleName(), name), columnType(spec))));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO `%s`
                    (%s, %s) VALUES (%s, %s);
             """.formatted(tableName(namespace), columnName("key"), columns.toString(), key, "? ".repeat(fieldMap.size())))) {

                statement.execute();
                return true;
            } catch (SQLException e) {

                // todo error
                return false;
            }
        });
    }
}
