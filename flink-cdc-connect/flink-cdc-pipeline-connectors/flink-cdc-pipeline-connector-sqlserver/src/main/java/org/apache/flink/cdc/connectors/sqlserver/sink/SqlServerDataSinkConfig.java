package org.apache.flink.cdc.connectors.sqlserver.sink;

import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;

import java.io.Serializable;

public class SqlServerDataSinkConfig extends JdbcSinkConfig implements Serializable {
    protected SqlServerDataSinkConfig(Builder builder) {
        super(builder);
    }

    public static class Builder extends JdbcSinkConfig.Builder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public SqlServerDataSinkConfig build() {
            return new SqlServerDataSinkConfig(this);
        }
    }
}
