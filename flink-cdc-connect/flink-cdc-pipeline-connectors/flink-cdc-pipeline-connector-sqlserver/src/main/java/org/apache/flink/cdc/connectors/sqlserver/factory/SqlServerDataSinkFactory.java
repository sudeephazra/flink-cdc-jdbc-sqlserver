package org.apache.flink.cdc.connectors.sqlserver.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions;
import org.apache.flink.cdc.connectors.jdbc.sink.JdbcDataSink;
import org.apache.flink.cdc.connectors.sqlserver.sink.SqlServerDataSinkConfig;
import org.apache.flink.cdc.connectors.sqlserver.sink.SqlServerPooledDataSinkFactory;
import org.apache.flink.cdc.connectors.sqlserver.sink.catalog.SqlServerCatalog;
import org.apache.flink.cdc.connectors.sqlserver.sink.catalog.SqlServerCatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.*;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.PASSWORD;

public class SqlServerDataSinkFactory implements DataSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerDataSinkFactory.class);

    public static final String IDENTIFIER = "sqlserver";
    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(JDBC_PROPERTIES_PROP_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        SqlServerDataSinkConfig.Builder builder = new SqlServerDataSinkConfig.Builder();
        config.getOptional(HOSTNAME).ifPresent(builder::hostname);
        config.getOptional(PORT).ifPresent(builder::port);
        config.getOptional(USERNAME).ifPresent(builder::username);
        config.getOptional(PASSWORD).ifPresent(builder::password);
        builder.serverTimeZone(config.getOptional(SERVER_TIME_ZONE).orElseGet(() -> "UTC"));
        builder.connectTimeout(
                config.getOptional(CONNECT_TIMEOUT).orElseGet(() -> Duration.ofSeconds(30)));
        builder.connectionPoolSize(config.getOptional(CONNECTION_POOL_SIZE).orElseGet(() -> 20));
        builder.connectMaxRetries(config.getOptional(CONNECT_MAX_RETRIES).orElseGet(() -> 3));
        // driver class name
        builder.driverClassName(
                config.getOptional(DRIVER_CLASS_NAME).orElseGet(() -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
        // builder jdbc properties
        Properties properties = new Properties();
        Map<String, String> jdbcProperties =
                JdbcSinkOptions.getPropertiesByPrefix(config, JDBC_PROPERTIES_PROP_PREFIX);
        properties.putAll(jdbcProperties);
        builder.jdbcProperties(properties);
        // get jdbc url
        String jdbcUrl = SqlServerPooledDataSinkFactory.INSTANCE.getJdbcUrl(builder.build());
        builder.connUrl(jdbcUrl);
        // print configs
        Map<String, String> map = config.toMap();
        OptionUtils.printOptions(IDENTIFIER, map);

        SqlServerDataSinkConfig sinkConfig = builder.build();
        SqlServerCatalog catalog = SqlServerCatalogFactory.INSTANCE.createCatalog(sinkConfig);

        return new JdbcDataSink(catalog, sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DRIVER_CLASS_NAME);
        options.add(SERVER_TIME_ZONE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECTION_POOL_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        return options;
    }
}
