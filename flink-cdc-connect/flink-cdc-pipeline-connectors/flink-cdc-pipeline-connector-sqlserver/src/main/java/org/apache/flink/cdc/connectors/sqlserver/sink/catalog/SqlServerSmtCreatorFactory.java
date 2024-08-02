package org.apache.flink.cdc.connectors.sqlserver.sink.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.jdbc.catalog.JdbcColumn;
import org.apache.flink.cdc.connectors.sqlserver.sink.type.SqlServerTypeTransformer;

import java.util.List;
import java.util.stream.Collectors;

public class SqlServerSmtCreatorFactory {

    public static final SqlServerSmtCreatorFactory INSTANCE = new SqlServerSmtCreatorFactory();

    // TODO: Check the SQL statement here
    private static final String RENAME_DDL = "ALTER TABLE %s RENAME COLUMN `%s` TO `%s`;";
    // TODO: Check the SQL statement here
    private static final String DROP_COLUMN_DDL = "ALTER TABLE %s DROP COLUMN `%s`;";

    public String buildUpsertSql(TableId tableId, List<Column> columns) {
        String tableName = tableId.identifier();
        // Building column names and value placeholders
        String columnNames =
                columns.stream().map(Column::getName).collect(Collectors.joining(", "));

        String valuePlaceholders =
                columns.stream().map(column -> "?").collect(Collectors.joining(", "));

        // Building the initial insert part
        StringBuilder query = new StringBuilder();
        // TODO: Check the SQL statement here
        query.append("INSERT INTO ")
                .append(tableName)
                .append(" (")
                .append(columnNames)
                .append(") VALUES (")
                .append(valuePlaceholders)
                .append(") ON DUPLICATE KEY UPDATE ");

        // Building the update part
        // TODO: Check the SQL statement here
        String updatePart =
                columns.stream()
                        .map(column -> column.getName() + " = VALUES(" + column.getName() + ")")
                        .collect(Collectors.joining(", "));

        query.append(updatePart).append(";");

        return query.toString();
    }

    public String buildDeleteSql(TableId tableId, List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        // TODO: Check the SQL statement here
        builder.append(String.format("DELETE FROM %s ", tableId.identifier()));
        builder.append("WHERE ");
        primaryKeys.forEach(
                pk -> {
                    builder.append(pk).append(" = ? AND ");
                });
        // remove latest " AND "
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }

    public String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addedColumns) {
        StringBuilder builder = new StringBuilder();
        // TODO: Check the SQL statement here
        builder.append(String.format("ALTER TABLE %s ", tableId.identifier()));

        String columnsStmt =
                addedColumns.stream()
                        .map(this::buildAddColumnStmt)
                        .collect(Collectors.joining(", "));

        builder.append(columnsStmt);
        builder.append(";");

        return builder.toString();
    }

    public String buildAlterColumnTypeSql(TableId tableId, String columnName, DataType columnType) {
        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        SqlServerTypeTransformer transformer = new SqlServerTypeTransformer(columnBuilder);
        columnType.accept(transformer);
        JdbcColumn type = columnBuilder.build();
        // TODO: Check the SQL statement here
        return String.format(
                "ALTER TABLE %s MODIFY COLUMN %s %s;",
                tableId.identifier(), columnName, type.getColumnType());
    }

    private String buildAddColumnStmt(AddColumnEvent.ColumnWithPosition columnWithPosition) {
        Column column = columnWithPosition.getAddColumn();
        StringBuilder builder = new StringBuilder();

        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        SqlServerTypeTransformer transformer = new SqlServerTypeTransformer(columnBuilder);
        column.getType().accept(transformer);
        JdbcColumn type = columnBuilder.build();
        // TODO: Check the SQL statement here
        builder.append("ADD COLUMN `")
                .append(column.getName())
                .append("` ")
                .append(type.getColumnType());

        if (column.getComment() != null) {
            builder.append(" COMMENT '").append(column.getComment()).append("'");
        }
        // TODO: Check the SQL statement here
        switch (columnWithPosition.getPosition()) {
            case FIRST:
                builder.append(" FIRST");
                break;
            case AFTER:
                builder.append(" AFTER `")
                        .append(columnWithPosition.getExistedColumnName())
                        .append("`");
                break;
            case BEFORE:
                builder.append(" BEFORE `")
                        .append(columnWithPosition.getExistedColumnName())
                        .append("`");
                break;
            case LAST:
            default:
                break;
        }

        return builder.toString();
    }

    public String buildRenameColumnSql(
            TableId tableId, String oldColumnName, String newColumnName) {
        return String.format(RENAME_DDL, tableId.identifier(), oldColumnName, newColumnName);
    }

    public String buildDropColumnSql(TableId tableId, String columnName) {
        return String.format(DROP_COLUMN_DDL, tableId.identifier(), columnName);
    }

    public String buildCreateTableSql(TableId tableId, Schema schema, boolean ignoreIfExists) {
        StringBuilder builder = new StringBuilder();
        // TODO: Check the SQL statement here
        builder.append(
                String.format(
                        "CREATE TABLE %s %s",
                        ignoreIfExists ? "IF NOT EXISTS" : "", tableId.identifier()));
        builder.append(" (\n");
        String columnsStmt =
                schema.getColumns().stream()
                        .map(this::buildColumnStmt)
                        .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);

        if (!schema.primaryKeys().isEmpty()) {
            builder.append(",\n");
            String tableKeys =
                    schema.primaryKeys().stream()
                            .map(key -> "`" + key + "`")
                            .collect(Collectors.joining(", "));
            builder.append(String.format("PRIMARY KEY (%s)", tableKeys));
        }
        builder.append("\n) ");
        builder.append(";");
        return builder.toString();
    }

    public String buildColumnStmt(Column column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getName());
        builder.append("` ");
        // build column type
        JdbcColumn.Builder columnBuilder = new JdbcColumn.Builder();
        SqlServerTypeTransformer transformer = new SqlServerTypeTransformer(columnBuilder);
        column.getType().accept(transformer);
        JdbcColumn type = columnBuilder.build();
        builder.append(type.getColumnType());

        if (!StringUtils.isNullOrWhitespaceOnly(column.getComment())) {
            // TODO: Check the SQL statement here
            builder.append(String.format(" COMMENT \"%s\"", column.getComment()));
        }
        return builder.toString();
    }
}
