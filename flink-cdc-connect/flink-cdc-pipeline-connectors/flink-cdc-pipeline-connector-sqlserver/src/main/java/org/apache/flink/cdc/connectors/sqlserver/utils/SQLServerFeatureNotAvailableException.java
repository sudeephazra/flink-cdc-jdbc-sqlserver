package org.apache.flink.cdc.connectors.sqlserver.utils;

import java.sql.SQLException;

public class SQLServerFeatureNotAvailableException extends SQLException {
    public SQLServerFeatureNotAvailableException(String message) {
        super(message);
    }

    public SQLServerFeatureNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
