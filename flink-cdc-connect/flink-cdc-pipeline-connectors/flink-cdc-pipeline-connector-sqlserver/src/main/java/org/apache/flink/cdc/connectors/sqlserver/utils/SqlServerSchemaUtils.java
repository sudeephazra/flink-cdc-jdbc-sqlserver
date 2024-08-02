package org.apache.flink.cdc.connectors.sqlserver.utils;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerUtils.quote;
import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.createSqlServerConnection;

public class SqlServerSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaUtils.class);

    public static List<String> listDatabases(SqlServerSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            SqlServerSourceConfig sourceConfig, @Nullable String dbName) {
        try (SqlServerConnection jdbc = createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbc);

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(
            SqlServerSourceConfig sourceConfig, SqlServerPartition partition, TableId tableId) {
        try (SqlServerConnection jdbc = createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        jdbc.query(
                "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'msdb', 'model', 'tempdb')",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String dbName) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // MySQL, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SELECT NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG=" + quote(dbName) + " AND TABLE_TYPE = 'BASE TABLE'",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(dbName, rs.getString(1)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(
            SqlServerPartition partition,
            TableId tableId,
            SqlServerSourceConfig sourceConfig,
            SqlServerConnection jdbc) {
        // fetch table schemas
            SqlServerSchema sqlServerSchema = new SqlServerSchema();
            TableChanges.TableChange tableSchema =
                    sqlServerSchema.getTableSchema(
                            jdbc,
                            toDbzTableId(tableId),
                            sourceConfig.getDbzConnectorConfig().getTableFilters().dataCollectionFilter()
                    );
            return toSchema(tableSchema.getTable());
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(SqlServerSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), SqlServerTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    private SqlServerSchemaUtils() {}
}
