package com.ververica.cdc.connectors.base.sink;

import java.util.Map;

interface DataChangeEvent extends Event {
    Payload before();
    Payload after();
    OperationType op(); // INSERT / UPDATE / DELETE / others (e.g. READ / INIT)
    Map<String, String> meta(); // Optional, e.g. MySQL binlog file name / pos
}