package com.ververica.cdc.connectors.base.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestValuesDataSink implements Sink<Event> {

    Map<TableID, InMemoryTable> tables = new ConcurrentHashMap<>();

    static class InMemoryTable {
        private final AtomicInteger fieldId = new AtomicInteger(-1);
        Map<String, Map<Column, String>> data = new ConcurrentHashMap<>();



        static class Column {
            int id;
            String type;
        }
    }


    @Override
    public SinkWriter<Event> createWriter(InitContext initContext) throws IOException {
        return null;
    }

    static class TestValuesSinkWriter implements SinkWriter<Event>, SupportSchemaEvolutionWriting {

        @Override
        public void processSchemaChangeEvent(SchemaChangeEvent event) throws IOException {

        }

        @Override
        public void write(Event event, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {

        }

        @Override
        public void close() throws Exception {

        }
    }
}
