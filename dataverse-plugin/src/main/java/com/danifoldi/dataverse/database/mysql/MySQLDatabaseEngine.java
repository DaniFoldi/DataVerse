package com.danifoldi.dataverse.database.mysql;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.microbase.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class MySQLDatabaseEngine extends SQLOperations {

    <T> CompletableFuture<Boolean> create(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    INSERT INTO ?
                    (?, %s) VALUES (?, %s);
             """.formatted(String.join(", ", Collections.nCopies(fieldMap.size(), "?")), String.join(", ", Collections.nCopies(fieldMap.size(), "?")));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);
                List<String> names = fieldMap.keySet().stream().toList();

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                setColumnNames(statement, names, fieldMap, c);
                statement.setString(c.getAndIncrement(), key);
                setStatementValues(statement, value, names, fieldMap, c);
                statement.execute();

                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }

    <T> CompletableFuture<T> get(String namespace, String key, T empty, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT %s
                    FROM ?
                    WHERE ? = ?
                      AND (? >= NOW() OR ? IS NULL);
             """.formatted(String.join(", ", Collections.nCopies(fieldMap.size(), "?")));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);
                List<String> names = fieldMap.keySet().stream().toList();

                setColumnNames(statement, names, fieldMap, c);
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {

                    return null;
                }
                setResultValues(results, empty, fieldMap);
                return empty;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return null;
            }
        });
    }

    CompletableFuture<List<String>> keys(String namespace) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL);
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                List<String> keys = new ArrayList<>();
                while (results.next()) {

                    keys.add(columnName(ColumnNames.KEY));
                }
                return keys;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    CompletableFuture<List<String>> keys(String namespace, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<String> keys = new ArrayList<>();
                while (results.next()) {

                    keys.add(columnName(ColumnNames.KEY));
                }
                return keys;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    CompletableFuture<List<String>> keys(String namespace, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<String> keys = new ArrayList<>();
                while (results.next()) {

                    keys.add(columnName(ColumnNames.KEY));
                }
                return keys;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL);
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);


                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> list(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMin(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(key, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(k, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(key, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(k, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(key, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterEquals(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T v = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, v, fieldMap);
                    values.add(Pair.of(k, v));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterMax(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, BigDecimal cutoff, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(key, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(k, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(key, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(k, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String key = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(key, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterBool(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, boolean value, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T instance = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, instance, fieldMap);
                    values.add(Pair.of(k, instance));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix, int pageCount, int pageLength) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

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
        });
    }

    <T> CompletableFuture<List<Pair<String, T>>> filterPrefix(String namespace, String key, Supplier<T> instanceSupplier, Map<String, FieldSpec> fieldMap, FieldSpec filterKey, String prefix, int pageCount, int pageLength, FieldSpec sortKey, boolean reverse) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?
                    ORDER BY ?
                    ?
                    LIMIT ?
                    OFFSET ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sortKey.type().toString(), sortKey.name()));
                statement.setString(c.getAndIncrement(), reverse ? "DESC" : "ASC");
                statement.setInt(c.getAndIncrement(), pageLength);
                statement.setInt(c.getAndIncrement(), (pageCount - 1) * pageLength);

                final @NotNull ResultSet results = statement.executeQuery();

                List<Pair<String, T>> values = new ArrayList<>();
                while (results.next()) {

                    T value = instanceSupplier.get();
                    String k = results.getString(columnName("key"));
                    setResultValues(results, value, fieldMap);
                    values.add(Pair.of(k, value));
                }
                return values;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return Collections.emptyList();
            }
        });
    }

    CompletableFuture<Long> count(String namespace) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> count(String namespace, String key) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT *
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterMin(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterMin(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterEquals(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterEquals(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterEquals(String namespace, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterEquals(String namespace, String key, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterMax(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterMax(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterBool(String namespace, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterBool(String namespace, String key, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterPrefix(String namespace, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countFilterPrefix(String namespace, String key, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinct(String namespace) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                    );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinct(String namespace, String key) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                    );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterMin(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterMin(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterEquals(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterEquals(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterEquals(String namespace, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterEquals(String namespace, String key, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterMax(String namespace, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterMax(String namespace, String key, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterBool(String namespace, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterBool(String namespace, String key, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterPrefix(String namespace, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<Long> countDistinctFilterPrefix(String namespace, String key, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT COUNT(*) as `count` FROM (
                    SELECT DISTINCT ?
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?
                      );
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return 0L;
                }
                return results.getLong("count");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return 0L;
            }
        });
    }

    CompletableFuture<BigDecimal> sum(String namespace, FieldSpec sumKey) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL);
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sum(String namespace, String key, FieldSpec sumKey) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterMin(String namespace, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? >= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterMin(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? >= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterEquals(String namespace, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterEquals(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterEquals(String namespace, FieldSpec sumKey, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterEquals(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, String value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterMax(String namespace, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? <= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterMax(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, BigDecimal cutoff) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? <= ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBigDecimal(c.getAndIncrement(), cutoff);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterBool(String namespace, FieldSpec sumKey, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterBool(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, boolean value) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setBoolean(c.getAndIncrement(), value);

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterPrefix(String namespace, FieldSpec sumKey, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? LIKE ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    CompletableFuture<BigDecimal> sumFilterPrefix(String namespace, String key, FieldSpec sumKey, FieldSpec filterKey, String prefix) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    SELECT SUM(?) as `sum`
                    FROM ?
                    WHERE (? >= NOW() OR ? IS NULL)
                      AND ? = ?
                      AND ? LIKE ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(sumKey.type().toString(), sumKey.name()));
                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                //noinspection UnstableApiUsage
                statement.setString(c.getAndIncrement(), columnName(filterKey.type().toString(), filterKey.name()));
                statement.setString(c.getAndIncrement(), prefix + "%");

                final @NotNull ResultSet results = statement.executeQuery();

                if (!results.next()) {
                    return BigDecimal.ZERO;
                }
                return results.getBigDecimal("sum");
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return BigDecimal.ZERO;
            }
        });
    }

    <T> CompletableFuture<Boolean> update(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    UPDATE ?
                    SET %s
                    WHERE ? = ?
                      AND (? >= NOW() OR ? IS NULL);
             """.formatted(String.join(", ", Collections.nCopies(fieldMap.size(), "? = ?")));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                setSelectRow(statement, value, fieldMap.keySet().stream().toList(), fieldMap, c);
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.execute();

                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }

    CompletableFuture<Boolean> delete(String namespace, String key) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    DELETE FROM ?
                    WHERE ? = ?
                      AND (? >= NOW() OR ? IS NULL);
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                statement.execute();

                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }

    <T> CompletableFuture<Boolean> deleteWhere(String namespace, String key, T value, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    DELETE FROM ?
                    WHERE ? = ?
                      AND %s
                      AND (? >= NOW() OR ? IS NULL);
             """.formatted(String.join(", ", Collections.nCopies(fieldMap.size(), "? = ?")));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);
                setStatementValues(statement, value, fieldMap.keySet().stream().toList(), fieldMap, new AtomicInteger(1));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));
                statement.execute();

                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }

    CompletableFuture<Boolean> expire(String namespace, String key, Instant expiry) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    UPDATE ?
                    SET ? = ?
                    WHERE ? = ?;
             """;

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                if (expiry == null) {

                    statement.setNull(c.getAndIncrement(), Types.TIMESTAMP);
                } else {

                    statement.setTimestamp(c.getAndIncrement(), Timestamp.from(expiry));
                }

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);

                statement.execute();
                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }

    <T> CompletableFuture<Boolean> expireWhere(String namespace, String key, T value, Instant expiry, Map<String, FieldSpec> fieldMap) {

        return CompletableFuture.supplyAsync(() -> {

            //language=MySQL
            String st = """
                    UPDATE ?
                    SET ? = ?
                    WHERE ? = ?
                      AND %s;
             """.formatted( String.join(" AND ", Collections.nCopies(fieldMap.size(), "? = ?")));

            try (final @NotNull Connection connection = connectionPool.getConnection();
                 final @NotNull PreparedStatement statement = connection.prepareStatement(st)) {

                AtomicInteger c = new AtomicInteger(1);

                statement.setString(c.getAndIncrement(), tableName(namespace));
                statement.setString(c.getAndIncrement(), columnName(ColumnNames.TTL_TIMESTAMP));

                if (expiry == null) {

                    statement.setNull(c.getAndIncrement(), Types.TIMESTAMP);
                } else {

                    statement.setTimestamp(c.getAndIncrement(), Timestamp.from(expiry));
                }

                statement.setString(c.getAndIncrement(), columnName(ColumnNames.KEY));
                statement.setString(c.getAndIncrement(), key);

                setSelectRow(statement, value, fieldMap.keySet().stream().toList(), fieldMap, c);

                statement.execute();
                return true;
            } catch (SQLException e) {

                logger.severe(e.getMessage());
                return false;
            }
        });
    }
}
