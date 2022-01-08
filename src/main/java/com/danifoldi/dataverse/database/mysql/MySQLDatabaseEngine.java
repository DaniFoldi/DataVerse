package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.google.common.collect.Multimap;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MySQLDatabaseEngine extends SQLOperations {

    <T> CompletableFuture<Boolean> create(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String columns = fieldMap.entrySet().stream().map(e -> "`%s`".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            //language=MySQL
            String st = """
                    INSERT INTO `%s`
                    (`%s`, %s) VALUES ("%s", %s);
             """.formatted(tableName(namespace), columnName(ColumnNames.KEY), columns, key, String.join(", ", Collections.nCopies(fieldMap.size(), "?")));

            return executeStatement(st, value, fieldMap);
        });
    }

    <T> CompletableFuture<T> get(String namespace, String key, T empty, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String columns = fieldMap.entrySet().stream().map(e -> "`%s`".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            //language=MySQL
            String st = """
                    SELECT %s
                    FROM `%s`
                    WHERE `%s` = "%s"
                      AND (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(columns, tableName(namespace), columnName(ColumnNames.KEY), key, columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeQuery(st, empty, fieldMap);
        });
    }

    CompletableFuture<List<String>> keys(String namespace) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT DISTINCT `%s`
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(columnName(ColumnNames.KEY), tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeKeyQuery(st);
        });
    }

    CompletableFuture<List<String>> keys(String namespace, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT `%s`
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    LIMIT %d
                    OFFSET %d;
             """.formatted(columnName(ColumnNames.KEY), tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), pageLength, (pageCount - 1) * pageLength);

            return executeKeyQuery(st);
        });
    }

    CompletableFuture<List<String>> keys(String namespace, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT `%s`
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    ORDER BY `%s`
                    %s
                    LIMIT %d
                    OFFSET %d;
             """.formatted(columnName(ColumnNames.KEY), tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), columnName(sortKey.type().toString(), sortKey.name()), reverse ? "DESC" : "ASC", pageLength, (pageCount - 1) * pageLength);

            return executeKeyQuery(st);
        });
    }

    <T> CompletableFuture<Map<String, T>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Map<String, T>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    LIMIT %d
                    OFFSET %d;
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), pageLength, (pageCount - 1) * pageLength);

            return executeListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Map<String, T>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    ORDER BY `%s`
                    %s
                    LIMIT %d
                    OFFSET %d;
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), columnName(sortKey.type().toString(), sortKey.name()), reverse ? "DESC" : "ASC", pageLength, (pageCount - 1) * pageLength);

            return executeListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Multimap<String, T>> multiList(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeMultiListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Multimap<String, T>> multiList(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    LIMIT %d
                    OFFSET %d;
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), pageLength, (pageCount - 1) * pageLength);

            return executeMultiListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Multimap<String, T>> multiList(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM `%s`
                    WHERE (`%s` >= NOW() OR `%s` IS NULL)
                    ORDER BY `%s`
                    %s
                    LIMIT %d
                    OFFSET %d;
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP), columnName(sortKey.type().toString(), sortKey.name()), reverse ? "DESC" : "ASC", pageLength, (pageCount - 1) * pageLength);

            return executeMultiListQuery(st, instanceSupplier, fieldMap);
        });
    }

    <T> CompletableFuture<Boolean> update(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String values = fieldMap.entrySet().stream().map(e -> "`%s` = ?".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(", "));
            //language=MySQL
            String st = """
                    UPDATE `%s`
                    SET %s
                    WHERE `%s` = "%s"
                      AND (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(tableName(namespace), values, columnName(ColumnNames.KEY), key, columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeStatement(st, value, fieldMap);
        });
    }

    CompletableFuture<Boolean> delete(String namespace, String key) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    DELETE FROM `%s`
                    WHERE `%s` = `%s`
                      AND (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(tableName(namespace), columnName(ColumnNames.KEY), key, columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeStatement(st);
        });
    }

    <T> CompletableFuture<Boolean> deleteWhere(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String values = fieldMap.entrySet().stream().map(e -> "`%s` = ?".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(" AND "));
            //language=MySQL
            String st = """
                    DELETE FROM `%s`
                    WHERE `%s` = `%s`
                      AND %s
                      AND (`%s` >= NOW() OR `%s` IS NULL);
             """.formatted(tableName(namespace), columnName(ColumnNames.KEY), key, values, columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.TTL_TIMESTAMP));

            return executeStatement(st, value, fieldMap);
        });
    }

    CompletableFuture<Boolean> expire(String namespace, String key, Instant expiry) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    UPDATE `%s`
                    SET `%s` = ?
                    WHERE `%s` = "%s";
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.KEY), key);

            return executeExpiryUpdate(st, expiry);
        });
    }

    <T> CompletableFuture<Boolean> expireWhere(String namespace, String key, T value, Instant expiry, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            String values = fieldMap.entrySet().stream().map(e -> "`%s` = ?".formatted(columnName(e.getValue().type().toString(), e.getKey()))).collect(Collectors.joining(" AND "));
            //language=MySQL
            String st = """
                    UPDATE `%s`
                    SET `%s` = ?
                    WHERE `%s` = "%s"
                      AND %s;
             """.formatted(tableName(namespace), columnName(ColumnNames.TTL_TIMESTAMP), columnName(ColumnNames.KEY), key, values);

            return executeExpiryUpdate(st, expiry);
        });
    }
}
