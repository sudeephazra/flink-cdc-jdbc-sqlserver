package org.apache.flink.cdc.connectors.sqlserver.sink.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.conn.JdbcConnectionPoolFactory;

import java.util.List;

public class SqlServerCatalog extends AbstractJdbcCatalog {

    public SqlServerCatalog(
            String name, JdbcSinkConfig sinkConfig, JdbcConnectionPoolFactory poolFactory) {
        super(name, sinkConfig, poolFactory);
    }
    @Override
    protected String buildUpsertSql(TableId tableId, Schema schema) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildUpsertSql(tableId, schema.getColumns());
    }

    @Override
    protected String buildDeleteSql(TableId tableId, List<String> primaryKeys) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildDeleteSql(tableId, primaryKeys);
    }

    @Override
    protected String buildCreateTableSql(TableId tableId, Schema schema, boolean ignoreIfExists) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildCreateTableSql(tableId, schema, ignoreIfExists);
    }

    @Override
    protected String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addColumnEvent) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildAlterAddColumnsSql(tableId, addColumnEvent);
    }

    @Override
    protected String buildRenameColumnSql(TableId tableId, String oldName, String newName) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildRenameColumnSql(tableId, oldName, newName);
    }

    @Override
    protected String buildDropColumnSql(TableId tableId, String column) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildDropColumnSql(tableId, column);
    }

    @Override
    protected String buildAlterColumnTypeSql(
            TableId tableId, String columnName, DataType columnType) {
        return SqlServerSmtCreatorFactory.INSTANCE.buildAlterColumnTypeSql(
                tableId, columnName, columnType);
    }
}
