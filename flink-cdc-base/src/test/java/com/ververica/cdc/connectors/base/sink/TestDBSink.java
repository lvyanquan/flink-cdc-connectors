/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * test DataSink
 */
public class TestDBSink implements Sink<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(TestDBSink.class);

    static Server server;

    public static void setUp() throws IOException {
        try {
            server = Server.createTcpServer( new String[] { "-tcp", "-tcpAllowOthers", "-tcpPort", "9092" }).start();
        } catch (SQLException e) {
            throw new IOException(e);
        }

    }

    public static void close() {
        if (server != null) {
            server.shutdown();
            server.stop();
        }
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext initContext) throws IOException {
        return new TestDBSinkWriter();
    }

    /** */
    static class TestDBSinkWriter implements SinkWriter<Event>, SupportSchemaEvolutionWriting {

        static String url = "jdbc:h2:tcp://localhost/mem:test;DB_CLOSE_DELAY=-1";
        final Connection conn;
        static Map<TableID, PreparedStatement> statementMap = new ConcurrentHashMap<>();
        PreparedStatement statement;

        TestDBSinkWriter() throws IOException {
            try {
                Class.forName("org.h2.Driver");
                conn = DriverManager.getConnection(url);
                conn.setAutoCommit(false);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void write(Event event, Context context) throws IOException, InterruptedException {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            try {
                if (!statementMap.containsKey(event.tableId())) {
                    PreparedStatement statement = conn.prepareStatement("");
                    statementMap.put(event.tableId(), statement);
                }
                statement = statementMap.get(event.tableId());
                statement.setObject(1, "");
                statement.addBatch();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    throw new IOException(e);
                }
                throw new IOException(e);
            }

        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {
            try {
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    throw new IOException(e);
                }
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws Exception {
            statementMap.values().forEach((statement) -> {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.error("", e);
                }
            });

            if (conn != null) {
                conn.close();
            }
        }

        @Override
        public void processSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
            statementMap.put(event.tableId(), buildPreparedStatement(event.tableId()));
        }

        PreparedStatement buildPreparedStatement(TableID tableID) throws IOException {
            try {
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery("show columns from ");
                PreparedStatement preparedStatement = conn.prepareStatement("");
                return preparedStatement;
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

    }

}
