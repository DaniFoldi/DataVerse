package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.translation.TranslationEngine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MySQLDatabaseEngine implements DatabaseEngine {

    HikariPool connectionPool;
    TranslationEngine translationEngine;
    Logger logger;

    @Override
    public void setLogger(@NotNull Logger logger) {

        this.logger = logger;
    }

    @Override
    public void connect(@NotNull Map<@NotNull String, @NotNull String> config, @NotNull TranslationEngine translationEngine) {

        this.translationEngine = translationEngine;
        String connectionUrl = String.format("jdbc:mysql://%s:%s/%s?%s",
                config.get("mysql_host"), config.get("mysql_port"), config.get("mysql_database"),
                config.get("mysql_connection_options").replaceFirst("^\\?", ""));
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
        hikariConfig.setDriverClassName(com.mysql.cj.jdbc.Driver.class.getName());
        hikariConfig.setUsername(config.get("mysql_user"));
        hikariConfig.setPassword(config.get("mysql_password"));
        hikariConfig.setJdbcUrl(connectionUrl);
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

        String st = """
                    CREATE EVENT IF NOT EXISTS
                      `%s`
                    ON SCHEDULE EVERY 1 HOUR
                    DO
                    DELETE FROM
                      `%s`
                    WHERE `%s` < NOW();
             """.formatted(eventName(namespace, "ttl"), tableName(namespace), columnName("ttl_timestamp"));
        logger.info("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            statement.execute();
        } catch (SQLException e) {

            // todo error
        }
    }

    private String tableName(String namespace) {

        return "%s__dataverse".formatted(namespace).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    private String eventName(String namespace, String event) {

        return "%s_%s_dataverse".formatted(namespace, event).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    private String columnName(String name) {

        return "dataverse_%s".formatted(name).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    private String columnName(String type, String name) {

        return "%s_%s".formatted(type, name).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    private void setStatementValues(PreparedStatement statement, Object value, Map<String, FieldSpec> fieldMap) {

        AtomicInteger i = new AtomicInteger(1);
        fieldMap.forEach((name, spec) -> {

            try {

                String typeName = spec.type().toString();
                translationEngine.getJavaTypeToMysqlQuery(typeName).apply(statement, i.getAndIncrement(), spec, value);
            } catch (ReflectiveOperationException | SQLException e) {

                // todo throw
                e.printStackTrace();
            }
        });
    }

    private void setResultValues(ResultSet result, Object value, Map<String, FieldSpec> fieldMap) {

        fieldMap.forEach((name, spec) -> {

            try {

                translationEngine.getMysqlResultToJavaType(spec.type().toString()).apply(result, columnName(spec.type().toString(), name), spec, value);
            } catch (ReflectiveOperationException | SQLException e) {

                // todo throw
                e.printStackTrace();
            }
        });
    }

    void createTable(String namespace, Map<String, FieldSpec> fieldMap) {

        StringBuilder columns = new StringBuilder();
        fieldMap.forEach((name, spec) -> columns.append("`%s` %s,\n".formatted(columnName(spec.type().toString(), name), translationEngine.getMysqlColumn(spec.type().toString()))));

        String st = """
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
             """.formatted(tableName(namespace), columnName("key"), columnName("create_timestamp"), columnName("update_timestamp"), columnName("ttl_timestamp"), columns.toString(), columnName("key"));
        logger.info("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            statement.execute();
        } catch (SQLException e) {

            System.out.println(e.getMessage());
            // todo error
        }
    }

    <T> CompletableFuture<Boolean> create(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {
        return CompletableFuture.supplyAsync(() -> {

            String columns = fieldMap.entrySet().stream().map(e -> "`%s`".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            String st = """
                    INSERT INTO `%s`
                    (`%s`, %s) VALUES ("%s", %s);
             """.formatted(tableName(namespace), columnName("key"), columns, key, String.join(", ", Collections.nCopies(fieldMap.size(), "?")));
            logger.info("Executing statement %s".formatted(st));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                setStatementValues(statement, value, fieldMap);
                statement.execute();

                return true;
            } catch (SQLException e) {

                // todo error
                System.out.println(e.getMessage());
                return false;
            }
        });
    }

    <T> CompletableFuture<T> get(String namespace, String key, T empty, Map<String, FieldSpec> fieldMap) {
        return CompletableFuture.supplyAsync(() -> {

            String columns = fieldMap.entrySet().stream().map(e -> "`%s`".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            String st = """
                    SELECT %s
                    FROM %s
                    WHERE `%s`="%s";
             """.formatted(columns, tableName(namespace), columnName("key"), key);
            logger.info("Executing statement %s".formatted(st));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st);
                 final @NotNull ResultSet results = statement.executeQuery()) {

                if (!results.next()) {

                    return null;
                }
                setResultValues(results, empty, fieldMap);
                return empty;
            } catch (SQLException e) {

                // todo error
                System.out.println(e.getMessage());
                return null;
            }
        });
    }

    CompletableFuture<Collection<String>> list(String namespace) {

        return CompletableFuture.supplyAsync(() -> {

            String st = """
                    SELECT `%s`
                    FROM `%s`;
             """.formatted(columnName("key"), tableName(namespace));
             logger.info("Executing statement %s".formatted(st));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st);
                 final @NotNull ResultSet results = statement.executeQuery()) {

                Set<String> keys = new LinkedHashSet<>();
                while (results.next()) {
                    keys.add(columnName("key"));
                }
                return keys;
            } catch (SQLException e) {

                // todo error
                System.out.println(e.getMessage());
                return Collections.emptySet();
            }
        });
    }

    <T> CompletableFuture<Boolean> update(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String values = fieldMap.entrySet().stream().map(e -> "`%s` = ?".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            String st = """
                    UPDATE `%s`
                    SET %s
                    WHERE `%s`="%s";
             """.formatted(tableName(namespace), values, columnName("key"), key);
            logger.info("Executing statement %s".formatted(st));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                setStatementValues(statement, value, fieldMap);
                statement.execute();
                return true;
            } catch (SQLException e) {

                // todo error
                System.out.println(e.getMessage());
                return false;
            }
        });
    }

    CompletableFuture<Boolean> delete(String namespace, String key) {

        return CompletableFuture.supplyAsync(() -> {

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement("""
                    DELETE FROM `%s`
                    WHERE `%s`=`%s`;
             """.formatted(tableName(namespace), columnName("key"), key))) {

                statement.execute();
                return true;
            } catch (SQLException e) {

                // todo error
                System.out.println(e.getMessage());
                return false;
            }
        });
    }
}
