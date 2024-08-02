package org.apache.flink.cdc.connectors.sqlserver.sink.catalog;

import org.apache.flink.cdc.connectors.jdbc.catalog.Catalog;
import org.apache.flink.cdc.connectors.jdbc.catalog.CatalogFactory;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSinkFactory;
import org.apache.flink.cdc.connectors.sqlserver.sink.SqlServerDataSinkConfig;
import org.apache.flink.cdc.connectors.sqlserver.sink.SqlServerPooledDataSinkFactory;

public class SqlServerCatalogFactory implements CatalogFactory<SqlServerDataSinkConfig> {

    public static final SqlServerCatalogFactory INSTANCE = new SqlServerCatalogFactory();
    @Override
    public SqlServerCatalog createCatalog(SqlServerDataSinkConfig option) {
        return new SqlServerCatalog(
                SqlServerDataSinkFactory.IDENTIFIER, option, SqlServerPooledDataSinkFactory.INSTANCE);
    }
}
