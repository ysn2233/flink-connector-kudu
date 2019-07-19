package org.nn.flink.streaming.connectors.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduMapper {

    private final static Map<Class, Type> kuduTypeMapping = new HashMap<>();

    public enum Mode {INSERT, UPDATE, UPSERT}

    static {
        kuduTypeMapping.put(Boolean.class, Type.BOOL);
        kuduTypeMapping.put(ByteBuffer.class, Type.BINARY);
        kuduTypeMapping.put(Double.class, Type.DOUBLE);
        kuduTypeMapping.put(Float.class, Type.FLOAT);
        kuduTypeMapping.put(String.class, Type.STRING);
        kuduTypeMapping.put(Byte.class, Type.INT8);
        kuduTypeMapping.put(Short.class, Type.INT16);
        kuduTypeMapping.put(Integer.class, Type.INT32);
        kuduTypeMapping.put(Long.class, Type.INT64);
        kuduTypeMapping.put(Date.class, Type.UNIXTIME_MICROS);
    }

    public static Type getKuduType(Object value) {
        return kuduTypeMapping.get(value.getClass());
    }

    public static Operation rowOperation(TableRow tableRow, KuduTable table, Mode mode) {
        Operation op;
        switch (mode) {
            case INSERT:
                op = table.newInsert();
                break;
            case UPDATE:
                op = table.newUpdate();
                break;
            case UPSERT:
                op = table.newUpsert();
                break;
            default:
                op = table.newUpsert();
        }
        List<ColumnSchema> columns = table.getSchema().getColumns();
        for (ColumnSchema columnSchema : columns) {
            PartialRow partialRow = op.getRow();
            String columnName = columnSchema.getName().toLowerCase();
            Type type = columnSchema.getType();
            if (tableRow.getPairs().containsKey(columnName)) {
                Object value = tableRow.getElement(columnName);
                rowAdd(partialRow, columnName, value, type);
            }
        }
        return op;
    }

    public static void rowAdd(PartialRow row, String key, Object value) {
        Type type =  getKuduType(value);
        rowAdd(row, key, value, type);
    }

    public static void rowAdd(PartialRow row, String key, Object value, Type type) {
        switch (type) {
            case BOOL:
                if (value instanceof String) {
                    row.addBoolean(key, Boolean.valueOf((String)value));
                } else {
                    row.addBoolean(key, (boolean)value);
                }
                break;
            case BINARY:
                row.addBinary(key, (ByteBuffer)value);
                break;
            case DOUBLE:
                if (value instanceof String) {
                    row.addDouble(key, Double.valueOf((String)value));
                } else {
                    row.addDouble(key, ((Number)value).doubleValue());
                }
                break;
            case STRING:
                row.addString(key, value.toString());
                break;
            case FLOAT:
                if (value instanceof String) {
                    row.addFloat(key, Float.valueOf((String)value));
                } else {
                    row.addFloat(key, ((Number)value).floatValue());
                }
                break;
            case INT8:
                if (value instanceof String) {
                    row.addByte(key, Byte.valueOf((String)value));
                } else {
                    row.addByte(key, ((Number)value).byteValue());
                }
                break;
            case INT16:
                if (value instanceof String) {
                    row.addShort(key, Short.valueOf((String)value));
                } else {
                    row.addShort(key, ((Number)value).shortValue());
                }
                break;
            case INT32:
                if (value instanceof String) {
                    row.addInt(key, Integer.valueOf((String)value));
                } else {
                    row.addInt(key, ((Number)value).intValue());
                }
                break;
            case INT64:
                if (value instanceof String) {
                    row.addLong(key, Long.valueOf((String)value));
                } else {
                    row.addLong(key, ((Number)value).longValue());
                }
                break;
            case UNIXTIME_MICROS:
                row.addLong(key, ((Date)value).getTime()*1000);
        }
    }
}
