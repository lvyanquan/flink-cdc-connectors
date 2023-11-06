package com.ververica.cdc.connectors.base.sink;

import io.debezium.schema.SchemaChangeEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ValuesDatabase {

    final static Map<TableID, InMemoryTable> tables = new ConcurrentHashMap<>();

    public static void delete(TableID tableID, String primaryKey) {
        validateTableExist(tableID);
        tables.get(tableID).delete(primaryKey);
    }

    public static void insert(TableID tableID, String primaryKey, Map<String, String> values) {
        validateTableExist(tableID);
        tables.get(tableID).delete(primaryKey);
        tables.get(tableID).insert(primaryKey, values);
    }

    public static void clearTables() {
        tables.clear();
    }

    private static void validateTableExist(TableID tableID) {
        if (tables.containsKey(tableID)) {
            throw new RuntimeException(tableID.toString() + " is not existed");
        }
    }

    public static void addColumn(TableID tableID, String name, String type) {
        validateTableExist(tableID);
        tables.get(tableID).addColumn(name, type);
    }

    public static void dropColumn(TableID tableID, String name) {
        validateTableExist(tableID);
        tables.get(tableID).dropColumn(name);
    }

    public static List<String> getResults(TableID tableID) {
        validateTableExist(tableID);
        return tables.get(tableID).getResult();
    }

    static class ValuesMetadataApplier implements MetadataApplier {

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {

        }
    }

    static class InMemoryTable {
        public final Object LOCK = new Object();
        Map<String, Map<Column, String>> data = new ConcurrentHashMap<>();
        TreeSet<Column> schema = new TreeSet<>(Comparator.comparingInt(column -> column.id));
        Map<Integer, Column> ColumnToNames = new ConcurrentHashMap<>();
        Map<String, Column> nameToColumns = new ConcurrentHashMap<>();
        final AtomicInteger fieldId = new AtomicInteger(-1);

        public List<String> getResult() {
            synchronized (LOCK) {
                List<String> results = new ArrayList<>();
                data.forEach((key, record) -> {
                            StringBuilder stringBuilder = new StringBuilder();
                            schema.forEach((column) -> {
                                        stringBuilder.append(record.getOrDefault(column, "")).append(",");
                                    }
                            );
                            stringBuilder.deleteCharAt(stringBuilder.length());
                            results.add(stringBuilder.toString());
                        }
                );
                return results;
            }
        }

        public void delete(String primaryKey) {
            data.remove(primaryKey);
        }

        public void insert(String primaryKey, Map<String, String> values) {
            Map<Column, String> record = new HashMap<>();
            values.forEach((key, value) ->{
                        record.put(nameToColumns.get(key), value);
                    }
            );
            data.put(primaryKey, record);
        }

        public void addColumn(String name, String type) {
            if (nameToColumns.containsKey(name)) {
                throw new RuntimeException("could not add the same column named " + name);
            }
            Column column = new Column(fieldId.incrementAndGet(), name, type);
            ColumnToNames.put(column.id, column);
            nameToColumns.put(name, column);
            schema.add(column);
        }

        public void dropColumn(String name) {
            if (!nameToColumns.containsKey(name)) {
                throw new RuntimeException("could not drop a not existed column named " + name);
            }
            Column column = nameToColumns.get(name);
            ColumnToNames.remove(column.id);
            nameToColumns.remove(name);
            schema.remove(column);
        }

        static class Column {
            int id;
            String name;
            String type;

            Column(int id, String name, String type) {
                this.id = id;
                this.name = name;
                this.type = type;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Column column = (Column) o;
                return id == column.id;
            }
        }
    }
}
