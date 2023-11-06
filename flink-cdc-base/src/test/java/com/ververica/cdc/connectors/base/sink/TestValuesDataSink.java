package com.ververica.cdc.connectors.base.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

public class TestValuesDataSink implements Sink<Event> {

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
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            switch (dataChangeEvent.op()) {
                case DELETE:{
                    ValuesDatabase.delete(dataChangeEvent.tableId(), dataChangeEvent.before().toString());
                }
                case INSERT:{
                    ValuesDatabase.insert(dataChangeEvent.tableId(), dataChangeEvent.after().toString(), null);
                }
                case UPDATE:{
                    ValuesDatabase.delete(dataChangeEvent.tableId(), dataChangeEvent.before().toString());
                    ValuesDatabase.insert(dataChangeEvent.tableId(), dataChangeEvent.after().toString(), null);
                }
            }

        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {

        }

        @Override
        public void close() throws Exception {

        }
    }


}
