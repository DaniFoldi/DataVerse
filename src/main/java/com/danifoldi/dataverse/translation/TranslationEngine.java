package com.danifoldi.dataverse.translation;

import com.danifoldi.dataverse.data.FieldSpec;
import com.danifoldi.dataverse.util.QuadConsumer;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TranslationEngine {

    private Map<String, String> javaTypeToMysqlColumn = new ConcurrentHashMap<>();
    private Map<String, QuadConsumer<PreparedStatement, Integer, FieldSpec, Object>> javaTypeToMysqlQuery = new ConcurrentHashMap<>();
    private Map<String, QuadConsumer<ResultSet, String, FieldSpec, Object>> mysqlResultToJavaType = new ConcurrentHashMap<>();

    public void addJavaTypeToMysqlColumn(String javaType, String mysqlColumn) {

        javaTypeToMysqlColumn.putIfAbsent(javaType, mysqlColumn);
    }

    public void addJavaTypeToMysqlQuery(String javaType, QuadConsumer<PreparedStatement, Integer, FieldSpec, Object> applier) {

        javaTypeToMysqlQuery.putIfAbsent(javaType, applier);
    }

    public void addMysqlResultToJavaType(String javaType, QuadConsumer<ResultSet, String, FieldSpec, Object> applier) {

        mysqlResultToJavaType.put(javaType, applier);
    }

    public String getMysqlColumn(String javaType) {

        return javaTypeToMysqlColumn.get(javaType);
    }

    public QuadConsumer<PreparedStatement, Integer, FieldSpec, Object> getJavaTypeToMysqlQuery(String javaType) {

        return javaTypeToMysqlQuery.get(javaType);
    }

    public QuadConsumer<ResultSet, String, FieldSpec, Object> getMysqlResultToJavaType(String javaType) {

        return mysqlResultToJavaType.get(javaType);
    }

    public void clear() {

        javaTypeToMysqlColumn.clear();
        javaTypeToMysqlQuery.clear();
    }

    public void setupStandard() {

        addJavaTypeToMysqlColumn("java.lang.String", "VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        addJavaTypeToMysqlColumn("java.lang.Integer", "INT");
        addJavaTypeToMysqlColumn("int", "INT");
        addJavaTypeToMysqlColumn("java.lang.Byte", "TINYINT");
        addJavaTypeToMysqlColumn("byte", "TINYINT");
        addJavaTypeToMysqlColumn("java.lang.Long", "BIGINT");
        addJavaTypeToMysqlColumn("long", "BIGINT");
        addJavaTypeToMysqlColumn("java.lang.Short", "SMALLINT");
        addJavaTypeToMysqlColumn("short", "SMALLINT");
        addJavaTypeToMysqlColumn("java.lang.Float", "FLOAT");
        addJavaTypeToMysqlColumn("float", "FLOAT");
        addJavaTypeToMysqlColumn("java.lang.Double", "DOUBLE");
        addJavaTypeToMysqlColumn("double", "DOUBLE");
        addJavaTypeToMysqlColumn("java.lang.Boolean", "BOOLEAN");
        addJavaTypeToMysqlColumn("boolean", "BOOLEAN");
        addJavaTypeToMysqlColumn("java.lang.Character", "CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        addJavaTypeToMysqlColumn("char", "CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        addJavaTypeToMysqlColumn("java.math.BigDecimal", "DECIMAL");
        addJavaTypeToMysqlColumn("java.util.UUID", "VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");

        addJavaTypeToMysqlQuery("java.lang.String", (statement, i, spec, obj) -> statement.setString(i, (String)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Integer", (statement, i, spec, obj) -> statement.setInt(i, (Integer)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("int", (statement, i, spec, obj) -> statement.setInt(i, (Integer)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Byte", (statement, i, spec, obj) -> statement.setByte(i, (Byte)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("byte", (statement, i, spec, obj) -> statement.setByte(i, (Byte)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Long", (statement, i, spec, obj) -> statement.setLong(i, (Long)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("long", (statement, i, spec, obj) -> statement.setLong(i, (Long)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Short", (statement, i, spec, obj) -> statement.setShort(i, (Short)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("short", (statement, i, spec, obj) -> statement.setShort(i, (Short)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Float", (statement, i, spec, obj) -> statement.setFloat(i, (Float)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("float", (statement, i, spec, obj) -> statement.setFloat(i, (Float)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Double", (statement, i, spec, obj) -> statement.setDouble(i, (Double)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("double", (statement, i, spec, obj) -> statement.setDouble(i, (Double)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Boolean", (statement, i, spec, obj) -> statement.setBoolean(i, (Boolean)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("boolean", (statement, i, spec, obj) -> statement.setBoolean(i, (Boolean)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.lang.Character", (statement, i, spec, obj) -> statement.setString(i, String.valueOf(spec.reflect().get(obj))));
        addJavaTypeToMysqlQuery("char", (statement, i, spec, obj) -> statement.setString(i, String.valueOf(spec.reflect().get(obj))));
        addJavaTypeToMysqlQuery("java.math.BigDecimal", (statement, i, spec, obj) -> statement.setBigDecimal(i, (BigDecimal)spec.reflect().get(obj)));
        addJavaTypeToMysqlQuery("java.util.UUID", (statement, i, spec, obj) -> statement.setString(i, spec.reflect().get(obj).toString()));

        addMysqlResultToJavaType("java.lang.String", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getString(colName)));
        addMysqlResultToJavaType("java.lang.Integer", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getInt(colName)));
        addMysqlResultToJavaType("int", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getInt(colName)));
        addMysqlResultToJavaType("java.lang.Byte", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getByte(colName)));
        addMysqlResultToJavaType("byte", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getByte(colName)));
        addMysqlResultToJavaType("java.lang.Long", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getLong(colName)));
        addMysqlResultToJavaType("long", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getLong(colName)));
        addMysqlResultToJavaType("java.lang.Short", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getShort(colName)));
        addMysqlResultToJavaType("short", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getShort(colName)));
        addMysqlResultToJavaType("java.lang.Float", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getFloat(colName)));
        addMysqlResultToJavaType("float", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getFloat(colName)));
        addMysqlResultToJavaType("java.lang.Double", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getDouble(colName)));
        addMysqlResultToJavaType("double", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getDouble(colName)));
        addMysqlResultToJavaType("java.lang.Boolean", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getBoolean(colName)));
        addMysqlResultToJavaType("boolean", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getBoolean(colName)));
        addMysqlResultToJavaType("java.lang.Character", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getString(colName)));
        addMysqlResultToJavaType("char", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getString(colName)));
        addMysqlResultToJavaType("java.math.BigDecimal", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getBigDecimal(colName)));
        addMysqlResultToJavaType("java.util.UUID", (results, colName, spec, obj) -> spec.reflect().set(obj, results.getString(colName)));
    }
}
