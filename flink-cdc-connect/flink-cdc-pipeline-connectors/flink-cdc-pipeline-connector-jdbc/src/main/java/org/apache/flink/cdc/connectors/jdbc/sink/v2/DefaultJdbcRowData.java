/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

/** implementation class for {@link JdbcRowData}. */
public class DefaultJdbcRowData implements JdbcRowData {
    private String uniqueKey;
    private String database;
    private TableId tableId;
    private Schema schema;
    private byte[] rows;

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setRows(byte[] rows) {
        this.rows = rows;
    }

    public DefaultJdbcRowData() {}

    public DefaultJdbcRowData(String uniqueKey, String database, TableId tableId, byte[] rows) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.tableId = tableId;
        this.rows = rows;
    }

    @Override
    public String getUniqueKey() {
        return uniqueKey;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public byte[] getRows() {
        return rows;
    }
}
