package org.apache.flink.cdc.connectors.sqlserver.sink;

import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.conn.JdbcConnectionPoolFactory;

import java.util.Properties;

public class SqlServerPooledDataSinkFactory extends JdbcConnectionPoolFactory {

    public static final SqlServerPooledDataSinkFactory INSTANCE = new SqlServerPooledDataSinkFactory();
    @Override
    public String getJdbcUrl(JdbcSinkConfig sinkConfig) {
        Properties jdbcProperties = sinkConfig.getJdbcProperties();
        StringBuilder url = new StringBuilder("jdbc:sqlserver://");
        url.append(sinkConfig.getHostname()).append(":").append(sinkConfig.getPort());
        url.append("?serverTimezone=").append(sinkConfig.getServerTimeZone());

        if (jdbcProperties != null && !jdbcProperties.isEmpty()) {
            for (String key : jdbcProperties.stringPropertyNames()) {
                url.append("&").append(key).append("=").append(jdbcProperties.getProperty(key));
            }
        }

        return url.toString();
    }
}
