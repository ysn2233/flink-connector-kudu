package org.nn.flink.streaming.connectors.kudu;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class KuduInsertUtils {

    private final static Map<Class, Type> kuduTypeMapping = new HashMap<>();

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

    public static void rowAdd(PartialRow row, String key, Object value) {
        Type type =  getKuduType(value);
        switch (type) {
            case BOOL:
                row.addBoolean(key, (Boolean)value);
                break;
            case BINARY:
                row.addBinary(key, (ByteBuffer)value);
                break;
            case DOUBLE:
                row.addDouble(key, (Double)value);
                break;
            case STRING:
                row.addString(key, (String)value);
                break;
            case FLOAT:
                row.addFloat(key, (Float)value);
                break;
            case INT8:
                row.addByte(key, (Byte)value);
                break;
            case INT16:
                row.addShort(key, (Short)value);
                break;
            case INT32:
                row.addInt(key, (Integer)value);
                break;
            case INT64:
                row.addLong(key, (Long) value);
                break;
            case UNIXTIME_MICROS:
                row.addLong(key, ((Date)value).getTime()*1000);
        }
    }
}
