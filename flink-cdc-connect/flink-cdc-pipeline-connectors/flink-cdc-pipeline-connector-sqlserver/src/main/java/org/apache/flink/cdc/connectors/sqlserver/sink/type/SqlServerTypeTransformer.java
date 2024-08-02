package org.apache.flink.cdc.connectors.sqlserver.sink.type;

import org.apache.flink.cdc.common.types.*;
import org.apache.flink.cdc.connectors.jdbc.catalog.JdbcColumn;

public class SqlServerTypeTransformer extends DataTypeDefaultVisitor<JdbcColumn.Builder> {

    private final JdbcColumn.Builder builder;
    public static final int POWER_2_8 = (int) Math.pow(2, 8);
    public static final int POWER_2_16 = (int) Math.pow(2, 16);
    public static final int POWER_2_24 = (int) Math.pow(2, 24);
    public static final int POWER_2_32 = (int) Math.pow(2, 32);

    public SqlServerTypeTransformer(JdbcColumn.Builder builder) {
        this.builder = builder;
    }

    @Override
    public JdbcColumn.Builder visit(CharType charType) {
        builder.length(charType.getLength());
//        builder.dataType(SqlServerType.CHAR.name());
        builder.dataType("CHAR");
        builder.columnType(charType.asSerializableString());
        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(VarCharType varCharType) {
        int length = varCharType.getLength();

        if (length < POWER_2_16 - 1) {
            builder.dataType("VARCHAR");
            builder.columnType(String.format("%s(%s)", "VARCHAR", length));
        } else if (length < POWER_2_24) {
            builder.dataType("MEDIUMTEXT");
            builder.columnType("MEDIUMTEXT");
        } else if (length < POWER_2_32) {
            builder.dataType("LONGTEXT");
            builder.columnType("LONGTEXT");
        } else {
            builder.dataType("TEXT");
            builder.columnType("TEXT");
        }

        builder.length(length);
        builder.isNullable(varCharType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BooleanType booleanType) {
        builder.dataType("TINYINT");
        builder.columnType("TINYINT(1)");
        builder.isNullable(booleanType.isNullable());
        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DecimalType decimalType) {
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();

        builder.dataType("DECIMAL");
        builder.columnType(decimalType.asSerializableString());
        builder.length(precision);
        builder.scale(scale);

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TinyIntType tinyIntType) {
        builder.dataType("TINYINT");
        builder.columnType(tinyIntType.asSerializableString());
        builder.isNullable(tinyIntType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(SmallIntType smallIntType) {
        builder.dataType("SMALLINT");
        builder.columnType(smallIntType.asSerializableString());
        builder.isNullable(smallIntType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(IntType intType) {
        builder.dataType("INT");
        builder.columnType(intType.asSerializableString());
        builder.isNullable(intType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BigIntType bigIntType) {
        builder.dataType("BIGINT");
        builder.columnType(bigIntType.asSerializableString());
        builder.isNullable(bigIntType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(FloatType floatType) {
        builder.dataType("FLOAT");
        builder.columnType(floatType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DoubleType doubleType) {
        builder.dataType("DOUBLE");
        builder.columnType(doubleType.asSerializableString());
        builder.isNullable(doubleType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BinaryType binaryType) {
        builder.dataType("BINARY");
        builder.length(binaryType.getLength());
        builder.columnType(binaryType.asSerializableString());
        builder.isNullable(binaryType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(VarBinaryType bytesType) {
        int length = bytesType.getLength();

        if (length <= POWER_2_16 - 1) {
            builder.dataType("VARBINARY");
            builder.columnType(String.format("%s(%d)", "VARBINARY", length));
        } else if (length < POWER_2_24) {
            builder.dataType("MEDIUMBLOB");
            builder.columnType("MEDIUMBLOB");
        } else if (length < POWER_2_32) {
            builder.dataType("LONGBLOB");
            builder.columnType("LONGBLOB");
        } else {
            builder.dataType("LONGBLOB");
            builder.columnType("LONGBLOB");
        }

        builder.length(length);
        builder.isNullable(bytesType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DateType dateType) {
        builder.dataType("DATE");
        builder.columnType(dateType.asSerializableString());
        builder.isNullable(dateType.isNullable());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TimeType timeType) {
        int precision = timeType.getPrecision();
        builder.length(precision);
        builder.dataType("TIME");
        if (precision > 0) {
            builder.columnType(timeType.asSerializableString());
        } else {
            builder.columnType("TIME");
        }

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TimestampType timestampType) {
        int precision = timestampType.getPrecision();
        builder.dataType("DATETIME");
        builder.length(precision);
        builder.isNullable(timestampType.isNullable());
        if (precision > 0) {
            builder.columnType(timestampType.asSerializableString());
        } else {
            builder.columnType("TIMESTAMP");
        }

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
        int precision = localZonedTimestampType.getPrecision();
        builder.dataType("TIMESTAMP");
        builder.length(precision);
        builder.isNullable(localZonedTimestampType.isNullable());
        if (precision > 0) {
            builder.columnType(String.format("TIMESTAMP(%d)", precision));
        } else {
            builder.columnType("TIMESTAMP");
        }

        return builder;
    }
    @Override
    protected JdbcColumn.Builder defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
    }
}
