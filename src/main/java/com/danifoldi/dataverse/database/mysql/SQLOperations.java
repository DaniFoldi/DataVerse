package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.database.DatabaseEngine;
import com.danifoldi.dataverse.translation.TranslationEngine;
import com.danifoldi.dataverse.util.Pair;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class SQLOperations implements DatabaseEngine {

    private HikariPool connectionPool;
    private TranslationEngine translationEngine;
    private Logger logger;

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

            logger.severe(e.getMessage());
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

            logger.severe(e.getMessage());
        }
    }

    void createTTLEvent(String namespace) {

        //language=MySQL
        String st = """
                    CREATE EVENT IF NOT EXISTS
                      `%s`
                    ON SCHEDULE EVERY 1 HOUR
                    DO
                    DELETE FROM
                      `%s`
                    WHERE `%s` < NOW();
             """.formatted(eventName(namespace, "ttl"), tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP));

        executeStatement(st);
    }

    String tableName(String namespace) {

        return "%s__dataverse".formatted(namespace).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    String eventName(String namespace, @SuppressWarnings("SameParameterValue") String event) {

        return "%s_%s_dataverse".formatted(namespace, event).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    String columnName(String name) {

        return "dataverse_%s".formatted(name).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    String columnName(String type, String name) {

        return "%s_%s".formatted(type, name).toLowerCase(Locale.ROOT).replaceAll("\s", "").replace(".",  "_");
    }

    <T> boolean executeStatement(String st, T value, Map<String, FieldSpec> fieldMap) {

        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            setStatementValues(statement, value, fieldMap);
            statement.execute();

            return true;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return false;
        }
    }

    boolean executeStatement(String st) {

        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            statement.execute();

            return true;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return false;
        }
    }

    <T> T executeQuery(String st, T empty, Map<String, FieldSpec> fieldMap) {
        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st);
             final @NotNull ResultSet results = statement.executeQuery()) {

            if (!results.next()) {

                return null;
            }
            setResultValues(results, empty, fieldMap);
            return empty;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return null;
        }
    }

    List<String> executeKeyQuery(String st) {

        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st);
             final @NotNull ResultSet results = statement.executeQuery()) {

            List<String> keys = new ArrayList<>();
            while (results.next()) {

                keys.add(columnName(ColumnNames.KEY));
            }
            return keys;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return Collections.emptyList();
        }
    }

    <T> List<Pair<String, T>> executeListQuery(String st, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap) {

        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st);
             final @NotNull ResultSet results = statement.executeQuery()) {

            List<Pair<String, T>> values = new ArrayList<>();
            while (results.next()) {

                T value = instanceSupplier.get();
                String key = results.getString(columnName("key"));
                setResultValues(results, value, fieldMap);
                values.add(Pair.of(key, value));
            }
            return values;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return Collections.emptyList();
        }
    }

    boolean executeExpiryUpdate(String st, Instant expiry) {

        logger.fine("Executing statement %s".formatted(st));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            if (expiry == null) {

                statement.setNull(1, Types.TIMESTAMP);
            } else {

                statement.setTimestamp(1, Timestamp.from(expiry));
            }
            statement.execute();
            return true;
        } catch (SQLException e) {

            logger.severe(e.getMessage());
            return false;
        }
    }

    void setStatementValues(PreparedStatement statement, Object value, Map<String, FieldSpec> fieldMap) {

        AtomicInteger i = new AtomicInteger(1);
        fieldMap.forEach((name, spec) -> {

            try {

                String typeName = spec.type().toString();
                translationEngine.getJavaTypeToMysqlQuery(typeName).apply(statement, i.getAndIncrement(), spec, value);
            } catch (ReflectiveOperationException | SQLException e) {

                logger.severe(e.getMessage());
            }
        });
    }

    void setResultValues(ResultSet result, Object value, Map<String, FieldSpec> fieldMap) {

        fieldMap.forEach((name, spec) -> {

            try {

                translationEngine.getMysqlResultToJavaType(spec.type().toString()).apply(result, columnName(spec.type().toString(), name), spec, value);
            } catch (ReflectiveOperationException | SQLException e) {

                logger.severe(e.getMessage());
            }
        });
    }

    void createTable(String namespace, Map<String, FieldSpec> fieldMap) {

        StringBuilder columns = new StringBuilder();
        fieldMap.forEach((name, spec) -> columns.append("`%s` %s,\n".formatted(columnName(spec.type().toString(), name), translationEngine.getMysqlColumn(spec.type().toString()))));

        //language=MySQL
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
             """.formatted(tableName(namespace), columnName(ColumnNames.KEY), columnName(ColumnNames.CREATE_TIMESTAMP), columnName(ColumnNames.UPDATE_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), columns.toString(), columnName(ColumnNames.KEY));

        executeStatement(st);
    }

    void createMultiTable(String namespace, Map<String, FieldSpec> fieldMap) {

        StringBuilder columns = new StringBuilder();
        fieldMap.forEach((name, spec) -> columns.append("`%s` %s,\n".formatted(columnName(spec.type().toString(), name), translationEngine.getMysqlColumn(spec.type().toString()))));

        //language=MySQL
        String st = """
                    CREATE TABLE IF NOT EXISTS
                    `%s`
                     (
                     `%s` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                     `%s` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     `%s` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     `%s` TIMESTAMP NULL DEFAULT NULL,
                     %s
                     
                     INDEX (`%s`)
                     )
                     ENGINE = InnoDB
                     CHARSET = utf8mb4
                     COLLATE utf8mb4_unicode_ci;
             """.formatted(tableName(namespace), columnName(ColumnNames.KEY), columnName(ColumnNames.CREATE_TIMESTAMP), columnName(ColumnNames.UPDATE_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), columns.toString(), columnName(ColumnNames.KEY));

        executeStatement(st);
    }
}
