package com.niceshot.hudi.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;

/**
 * @author created by chenjun at 2020-10-26 13:54
 */
public class SqlTypeUtils {
    public static Object convertSqlStringValue2JavaTypeObj(String data,Integer sqlType) {
        Object result = null;

        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                result = data;
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                result = new BigDecimal(data);
                break;

            case Types.BIT:
                result = Boolean.valueOf(data);
                break;

            case Types.TINYINT:
                result = Byte.valueOf(data);
                break;

            case Types.SMALLINT:
                result = Short.valueOf(data);
                break;

            case Types.INTEGER:
                result = Integer.valueOf(data);
                break;

            case Types.BIGINT:
                result = Long.valueOf(data);
                break;

            case Types.REAL:
            case Types.FLOAT:
                result = Float.valueOf(data);
                break;

            case Types.DOUBLE:
                result = Double.valueOf(data);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                throw new RuntimeException("unsupport data type now!!!!!");

            case Types.DATE:
                result = Date.valueOf(data);
                break;

            case Types.TIME:
                result = java.sql.Time.valueOf(data);
                break;

            case Types.TIMESTAMP:
                result = java.sql.Timestamp.valueOf(data);
                break;
        }

        return result;
    }
}
