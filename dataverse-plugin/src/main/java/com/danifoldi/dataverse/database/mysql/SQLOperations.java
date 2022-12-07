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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class SQLOperations implements DatabaseEngine {

    protected HikariPool connectionPool;
    protected TranslationEngine translationEngine;
    protected Logger logger;

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
                      ?
                    ON SCHEDULE EVERY 1 HOUR
                    DO
                    DELETE FROM
                      ?
                    WHERE ? < NOW();
                    """;

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            statement.setString(1, eventName(namespace, "ttl"));
            statement.setString(2, tableName(namespace));
            statement.setString(3, columnName(ColumnNames.TTL_TIMESTAMP));

            statement.execute();
        } catch (SQLException e) {

            logger.severe(e.getMessage());
        }
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

    void setColumnNames(PreparedStatement statement, List<String> names, Map<String, FieldSpec> fieldMap, AtomicInteger c) {

        try {

            for (String name: names) {

                statement.setString(c.getAndIncrement(), columnName(fieldMap.get(name).type().toString(), name));
            }
        } catch (SQLException e) {

            logger.severe(e.getMessage());
        }
    }

    void setStatementValues(PreparedStatement statement, Object value, List<String> names, Map<String, FieldSpec> fieldMap, AtomicInteger i) {

        try {

            for (String name: names) {

                translationEngine.getJavaTypeToMysqlQuery(fieldMap.get(name).type().toString()).apply(statement, i.getAndIncrement(), fieldMap.get(name), value);
            }
        } catch (ReflectiveOperationException | SQLException e) {

            logger.severe(e.getMessage());
        }
    }

    void setSelectRow(PreparedStatement statement, Object value, List<String> names, Map<String, FieldSpec> fieldMap, AtomicInteger i) {

        try {

            for (String name: names) {

                statement.setString(i.getAndIncrement(), columnName(fieldMap.get(name).type().toString(), name));
                translationEngine.getJavaTypeToMysqlQuery(fieldMap.get(name).type().toString()).apply(statement, i.getAndIncrement(), fieldMap.get(name), value);
            }
        } catch (ReflectiveOperationException | SQLException e) {

            logger.severe(e.getMessage());
        }
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

        //language=MySQL
        String st = """
                    CREATE TABLE IF NOT EXISTS
                    ?
                     (
                     ? VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                     ? TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     ? TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     ? TIMESTAMP NULL DEFAULT NULL,
                     %s
                     
                     PRIMARY KEY (?)
                     )
                     ENGINE = InnoDB
                     CHARSET = utf8mb4
                     COLLATE utf8mb4_unicode_ci;
             """.formatted("? ?,\n".repeat(fieldMap.size()));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            AtomicInteger c = new AtomicInteger(6);

            statement.setString(c.getAndIncrement(), tableName(namespace));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.CREATE_TIMESTAMP));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.UPDATE_TIMESTAMP));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

            for (Map.Entry<String, FieldSpec> field: fieldMap.entrySet()) {

                statement.setString(c.getAndIncrement(), columnName(field.getValue().type().toString(), field.getKey()));
                statement.setString(c.getAndIncrement(), translationEngine.getMysqlColumn(field.getValue().type().toString()));
            }

            statement.setString(c.get(), columnName(ColumnNames.KEY));

            statement.execute();
        } catch (SQLException e) {

            logger.severe(e.getMessage());
        }
    }

    void createMultiTable(String namespace, Map<String, FieldSpec> fieldMap) {

        //language=MySQL
        String st = """
                    CREATE TABLE IF NOT EXISTS
                    ?
                     (
                     ? VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                     ? TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     ? TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                     ? TIMESTAMP NULL DEFAULT NULL,
                     %s
                     
                     INDEX (?)
                     )
                     ENGINE = InnoDB
                     CHARSET = utf8mb4
                     COLLATE utf8mb4_unicode_ci;
             """.formatted("? ?,\n".repeat(fieldMap.size()));

        try (final @NotNull Connection connection = connectionPool.getConnection();
             final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

            AtomicInteger c = new AtomicInteger(1);

            statement.setString(c.getAndIncrement(), tableName(namespace));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.CREATE_TIMESTAMP));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.UPDATE_TIMESTAMP));
            statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

            for (Map.Entry<String, FieldSpec> field: fieldMap.entrySet()) {

                statement.setString(c.getAndIncrement(), columnName(field.getValue().type().toString(), field.getKey()));
                statement.setString(c.getAndIncrement(), translationEngine.getMysqlColumn(field.getValue().type().toString()));
            }

            statement.setString(c.get(), columnName(ColumnNames.KEY));

            statement.execute();
        } catch (SQLException e) {

            logger.severe(e.getMessage());
        }
    }
}
